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

import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real loopback TCP tests with controlled PostgreSQL wire messages; no database or Docker required.
 */
@Timeout(15)
final class ReactorNettyClientResponseTimeoutTests {

    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private static final Duration LIMIT = Duration.ofSeconds(8);

    private final Sinks.Many<byte[]> responses = Sinks.many().unicast().onBackpressureBuffer();

    private final AtomicInteger requests = new AtomicInteger();

    private final AtomicInteger terminateWrites = new AtomicInteger();

    private final AtomicReference<Connection> acceptedConnection = new AtomicReference<>();

    private final DisposableServer server = TcpServer.create().host("127.0.0.1").port(0)
        .doOnConnection(this.acceptedConnection::set)
        .handle((in, out) -> Mono.when(in.receive().doOnNext(bytes -> {
            this.requests.incrementAndGet();
            if (bytes.getByte(bytes.readerIndex()) == 'X') {
                this.terminateWrites.incrementAndGet();
            }
        }).then(), out.sendByteArray(this.responses.asFlux()).then()))
        .bindNow(LIMIT);

    @AfterEach
    void cleanup() {
        Connection accepted = this.acceptedConnection.get();
        if (accepted != null) {
            accepted.disposeNow(LIMIT);
        }
        this.server.disposeNow(LIMIT);
    }

    @Test
    void silenceFailsAllConversationsClosesSocketAndRejectsReuse() {
        ReactorNettyClient client = client(TIMEOUT);
        AtomicReference<Throwable> first = new AtomicReference<>();
        AtomicReference<Throwable> second = new AtomicReference<>();
        exchange(client).subscribe(message -> {
        }, first::set);
        exchange(client).subscribe(message -> {
        }, second::set);

        await().atMost(LIMIT).untilAsserted(() -> {
            assertThat(first.get()).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessageContaining("No inbound response");
            assertThat(second.get()).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessageContaining("No inbound response");
            assertThat(serverChannelOpen()).isFalse();
        });
        assertThat(((R2dbcNonTransientResourceException) first.get()).getSqlState()).isEqualTo("08006");
        assertThat(client.isConnected()).isFalse();
        StepVerifier.create(exchange(client)).expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
    }

    @Test
    void idleConnectionSurvivesAndFirstRequestGetsNewWindow() {
        ReactorNettyClient client = client(TIMEOUT);
        await().during(TIMEOUT.multipliedBy(2)).atMost(LIMIT).until(client::isConnected);
        StepVerifier.create(exchange(client))
            .expectSubscription().expectNoEvent(TIMEOUT.dividedBy(2))
            .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
    }

    @Test
    void disabledTimeoutLeavesSilentOperationPending() {
        ReactorNettyClient client = client(null);
        StepVerifier.create(exchange(client)).expectSubscription()
            .expectNoEvent(TIMEOUT.multipliedBy(2)).thenCancel().verify(LIMIT);
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void regularResponsesAllowStreamLongerThanTimeoutThenSilenceFails() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client))
            .then(this::awaitRequest)
            .then(() -> send(commandComplete()))
            .expectNextCount(1).thenAwait(Duration.ofMillis(350))
            .then(() -> send(commandComplete()))
            .expectNextCount(1).thenAwait(Duration.ofMillis(350))
            .then(() -> send(commandComplete()))
            .expectNextCount(1).thenAwait(Duration.ofMillis(350))
            .then(() -> send(commandComplete()))
            .expectNextCount(1)
            .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
    }

    @Test
    void partialFramesCountAsInboundActivity() {
        ReactorNettyClient client = client(TIMEOUT);
        byte[] frame = commandComplete();
        StepVerifier.create(exchange(client))
            .then(this::awaitRequest)
            .then(() -> send(new byte[]{frame[0]})).thenAwait(Duration.ofMillis(350))
            .then(() -> send(new byte[]{frame[1]})).thenAwait(Duration.ofMillis(350))
            .then(() -> send(new byte[]{frame[2]})).thenAwait(Duration.ofMillis(350))
            .then(() -> send(java.util.Arrays.copyOfRange(frame, 3, frame.length)))
            .expectNextCount(1).then(() -> send(ready()))
            .expectComplete().verify(LIMIT);
    }

    @Test
    void backpressureDoesNotTurnBufferedCompletionIntoNetworkFailure() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client), 0)
            .then(this::awaitRequest)
            .then(() -> send(commandComplete())).then(() -> send(ready()))
            .thenAwait(TIMEOUT.multipliedBy(2))
            .then(() -> assertThat(client.isConnected()).isTrue())
            .thenRequest(1).expectNextCount(1)
            .thenRequest(1).expectComplete().verify(LIMIT);
        await().during(TIMEOUT.multipliedBy(2)).atMost(LIMIT).until(client::isConnected);
    }

    @Test
    void drainingBufferedDataRestartsTimeoutWhenResponseStillOutstanding() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client), 0)
            .then(this::awaitRequest).then(() -> send(commandComplete()))
            .thenAwait(TIMEOUT.multipliedBy(2))
            .then(() -> assertThat(client.isConnected()).isTrue())
            .thenRequest(1).expectNextCount(1)
            .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
    }

    @Test
    void subscriberCancellationDoesNotDisableConnectionSafeguard() {
        ReactorNettyClient client = client(TIMEOUT);
        Disposable subscription = exchange(client).subscribe();
        awaitRequest();
        subscription.dispose();
        await().atMost(LIMIT).until(() -> !serverChannelOpen());
    }

    @Test
    void normalCompletionLeavesIdleConnectionOpen() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client)).then(this::awaitRequest)
            .then(() -> send(ready())).expectComplete().verify(LIMIT);
        await().during(TIMEOUT.multipliedBy(2)).atMost(LIMIT).until(client::isConnected);
        client.close().block(LIMIT);
        await().atMost(LIMIT).until(() -> !serverChannelOpen());
    }

    @Test
    void timeoutBypassesGracefulTerminateForEstablishedSession() {
        ReactorNettyClient client = client(TIMEOUT);
        send(ByteBuffer.allocate(13).put((byte) 'K').putInt(12).putInt(123).putInt(456).array());
        await().atMost(LIMIT).until(() -> client.getProcessId().isPresent());
        StepVerifier.create(exchange(client)).expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
        await().atMost(LIMIT).until(() -> !serverChannelOpen());
        assertThat(this.terminateWrites).hasValue(0);
    }

    @Test
    void slowConsumptionOnAnotherThreadPreservesCompletedStream() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client).publishOn(reactor.core.scheduler.Schedulers.parallel(), 1).delayElements(Duration.ofMillis(50)))
            .then(this::awaitRequest)
            .then(() -> {
                for (int i = 0; i < 12; i++) {
                    send(commandComplete());
                }
                send(ready());
            }).expectNextCount(12).expectComplete().verify(LIMIT);
        await().during(TIMEOUT.multipliedBy(2)).atMost(LIMIT).until(client::isConnected);
    }

    @Test
    void cancellationDrainsBufferedDataAndResumesSilenceDetection() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(exchange(client), 0).then(this::awaitRequest)
            .then(() -> send(commandComplete())).thenAwait(TIMEOUT.multipliedBy(2))
            .then(() -> assertThat(client.isConnected()).isTrue()).thenCancel().verify(LIMIT);
        await().atMost(LIMIT).until(() -> !serverChannelOpen());
    }

    @Test
    void zeroDisablesTimeout() {
        ReactorNettyClient client = client(Duration.ZERO);
        StepVerifier.create(exchange(client)).expectSubscription().expectNoEvent(TIMEOUT.multipliedBy(2)).thenCancel().verify(LIMIT);
        assertThat(client.isConnected()).isTrue();
    }

    @Test
    void delayedRequestPublisherDoesNotStartResponseDeadlineEarly() {
        ReactorNettyClient client = client(TIMEOUT);
        StepVerifier.create(client.exchange(Mono.delay(TIMEOUT.multipliedBy(2)).map(it -> new Query("SELECT 1"))))
            .expectSubscription().expectNoEvent(TIMEOUT.multipliedBy(2))
            .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
    }

    private ReactorNettyClient client(@Nullable Duration timeout) {
        ReactorNettyClient client = ReactorNettyClient.connect(new InetSocketAddress("127.0.0.1", this.server.port()),
            ConnectionSettings.builder().responseTimeout(timeout).build()).block(LIMIT);
        await().atMost(LIMIT).until(() -> this.acceptedConnection.get() != null);
        return client;
    }

    private boolean serverChannelOpen() {
        return Objects.requireNonNull(this.acceptedConnection.get()).channel().isOpen();
    }

    private reactor.core.publisher.Flux<BackendMessage> exchange(ReactorNettyClient client) {
        return client.exchange(Mono.just(new Query("SELECT 1")));
    }

    private void awaitRequest() {
        await().atMost(LIMIT).until(() -> this.requests.get() > 0);
    }

    private void send(byte[] data) {
        this.responses.emitNext(data, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    private static byte[] commandComplete() {
        byte[] tag = "SELECT 1\0".getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(5 + tag.length).put((byte) 'C').putInt(4 + tag.length).put(tag).array();
    }

    private static byte[] ready() {
        return new byte[]{'Z', 0, 0, 0, 5, 'I'};
    }

}
