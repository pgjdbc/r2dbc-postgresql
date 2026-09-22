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

import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Connection-wide response inactivity watchdog. Incoming bytes move the deadline without
 * allocating a timer per message. Buffering pauses the deadline until the client can read again.
 */
final class ResponseInactivityTimeout implements Disposable {

    private final long timeoutNanos;

    private final Scheduler scheduler;

    private final LongSupplier clock;

    private final Runnable onTimeout;

    private @Nullable Disposable task;

    private int conversations;

    private long lastReceived;

    private long generation;

    private boolean paused;

    private boolean inputPending;

    private boolean disposed;

    ResponseInactivityTimeout(Duration timeout, Scheduler scheduler, LongSupplier clock, Runnable onTimeout) {
        Assert.isTrue(!timeout.isNegative() && !timeout.isZero(), "Response timeout must be positive");
        this.timeoutNanos = timeout.toNanos();
        this.scheduler = scheduler;
        this.clock = clock;
        this.onTimeout = onTimeout;
    }

    synchronized void conversationStarted() {
        if (!this.disposed && this.conversations++ == 0) {
            this.lastReceived = this.clock.getAsLong();
            if (!this.paused && !this.inputPending) {
                schedule(this.timeoutNanos);
            }
        }
    }

    synchronized void conversationCompleted() {
        if (!this.disposed && --this.conversations == 0) {
            cancelTask();
        }
    }

    synchronized void onResponse() {
        if (!this.disposed) {
            this.lastReceived = this.clock.getAsLong();
        }
    }

    synchronized void pause() {
        if (!this.disposed && !this.paused) {
            this.paused = true;
            cancelTask();
        }
    }

    synchronized void resume() {
        if (!this.disposed && this.paused) {
            this.paused = false;
            this.lastReceived = this.clock.getAsLong();
            if (this.conversations > 0 && !this.inputPending) {
                schedule(this.timeoutNanos);
            }
        }
    }

    synchronized void inputPending(boolean inputPending) {
        if (this.disposed || this.inputPending == inputPending) {
            return;
        }
        this.inputPending = inputPending;
        if (inputPending) {
            cancelTask();
        } else if (!this.paused && this.conversations > 0) {
            this.lastReceived = this.clock.getAsLong();
            schedule(this.timeoutNanos);
        }
    }

    private void schedule(long delay) {
        long token = ++this.generation;
        this.task = this.scheduler.schedule(() -> check(token), delay, TimeUnit.NANOSECONDS);
    }

    private void check(long token) {
        synchronized (this) {
            if (this.disposed || this.paused || this.inputPending || this.conversations == 0 || token != this.generation) {
                return;
            }
            long remaining = this.timeoutNanos - (this.clock.getAsLong() - this.lastReceived);
            if (remaining > 0) {
                schedule(remaining);
                return;
            }
            dispose();
        }
        // Do not call client code under the watchdog lock.
        this.onTimeout.run();
    }

    private void cancelTask() {
        this.generation++;
        if (this.task != null) {
            this.task.dispose();
            this.task = null;
        }
    }

    @Override
    public synchronized void dispose() {
        this.disposed = true;
        cancelTask();
    }

    @Override
    public synchronized boolean isDisposed() {
        return this.disposed;
    }

}
