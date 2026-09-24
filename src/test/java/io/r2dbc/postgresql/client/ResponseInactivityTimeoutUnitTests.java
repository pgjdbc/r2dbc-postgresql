/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ResponseInactivityTimeoutUnitTests {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    private final AtomicInteger expirations = new AtomicInteger();

    private final ResponseInactivityTimeout timeout = new ResponseInactivityTimeout(Duration.ofSeconds(10), this.scheduler,
        () -> this.scheduler.now(TimeUnit.NANOSECONDS), this.expirations::incrementAndGet);

    @AfterEach
    void cleanup() {
        this.timeout.dispose();
        this.scheduler.dispose();
    }

    @Test
    void idleThenFirstRequestGetsFullWindow() {
        advance(100);
        assertThat(this.expirations).hasValue(0);
        this.timeout.conversationStarted();
        advance(9);
        assertThat(this.expirations).hasValue(0);
        advance(1);
        assertThat(this.expirations).hasValue(1);
        advance(100);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void inboundActivityExtendsWindowWithoutTotalQueryDeadline() {
        this.timeout.conversationStarted();
        for (int i = 0; i < 20; i++) {
            advance(9);
            this.timeout.onResponse();
        }
        advance(9);
        assertThat(this.expirations).hasValue(0);
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void queuedRequestsDoNotPostponeExistingSilence() {
        this.timeout.conversationStarted();
        advance(9);
        this.timeout.conversationStarted();
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void completingOneConversationKeepsDeadlineForOthers() {
        this.timeout.conversationStarted();
        this.timeout.conversationStarted();
        advance(9);
        this.timeout.conversationCompleted();
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void completedConversationCancelsOldDeadlineAndNextRequestStartsFresh() {
        this.timeout.conversationStarted();
        advance(9);
        this.timeout.conversationCompleted();
        advance(100);
        this.timeout.conversationStarted();
        advance(9);
        assertThat(this.expirations).hasValue(0);
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void backpressurePausesAndResumeGetsFreshWindow() {
        this.timeout.conversationStarted();
        advance(9);
        this.timeout.pause();
        advance(100);
        assertThat(this.expirations).hasValue(0);
        this.timeout.resume();
        advance(9);
        assertThat(this.expirations).hasValue(0);
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void closeCancelsTimerAndCannotBeRearmed() {
        this.timeout.conversationStarted();
        this.timeout.dispose();
        this.timeout.onResponse();
        this.timeout.conversationStarted();
        this.timeout.pause();
        this.timeout.resume();
        advance(100);
        assertThat(this.expirations).hasValue(0);
        assertThat(this.timeout.isDisposed()).isTrue();
    }

    @Test
    void activityJustBeforeDeadlinePreventsStaleExpiry() {
        this.timeout.conversationStarted();
        this.scheduler.advanceTimeBy(Duration.ofSeconds(10).minusNanos(1));
        this.timeout.onResponse();
        this.scheduler.advanceTimeBy(Duration.ofNanos(1));
        assertThat(this.expirations).hasValue(0);
        advance(10);
        assertThat(this.expirations).hasValue(1);
    }

    @Test
    void rejectsNonPositiveDurations() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ResponseInactivityTimeout(Duration.ZERO, this.scheduler, System::nanoTime, () -> {
        }));
        assertThatIllegalArgumentException().isThrownBy(() -> new ResponseInactivityTimeout(Duration.ofSeconds(-1), this.scheduler, System::nanoTime, () -> {
        }));
    }

    @Test
    void clientInputAndBackpressureMustBothResumeBeforeTimeout() {
        this.timeout.conversationStarted();
        this.timeout.inputPending(true);
        this.timeout.pause();
        advance(100);
        this.timeout.resume();
        advance(100);
        assertThat(this.expirations).hasValue(0);
        this.timeout.inputPending(false);
        advance(9);
        assertThat(this.expirations).hasValue(0);
        advance(1);
        assertThat(this.expirations).hasValue(1);
    }

    private void advance(long seconds) {
        this.scheduler.advanceTimeBy(Duration.ofSeconds(seconds));
    }

}
