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

package io.r2dbc.postgresql;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.util.TestByteBufAllocator;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PostgresqlStatement} verifying that the {@link Bind} parameter buffers of a statement that never reaches the wire are released.
 * <p>
 * The statements run against a real {@link ReactorNettyClient} connected to a TCP server that accepts the connection and never replies, which is enough to exercise the driver's outbound path without
 * a Postgres server.
 *
 * @see <a href="https://github.com/pgjdbc/r2dbc-postgresql/issues/734">gh-734</a>
 */
final class PostgresqlStatementCancellationUnitTests {

    private static final String SQL = "INSERT INTO t (s) VALUES ($1)";

    private static final int ROWS = 40;

    private static final Field CONNECTION = ReflectionUtils.findField(ReactorNettyClient.class, "connection");

    static {
        ReflectionUtils.makeAccessible(CONNECTION);
    }

    private final List<ByteBuf> parameters = new CopyOnWriteArrayList<>();

    private DisposableServer server;

    private ReactorNettyClient client;

    private ConnectionResources resources;

    @BeforeEach
    void before() {

        // A server that accepts the connection, drains whatever the driver writes and never replies.
        this.server = TcpServer.create().host("localhost").handle((in, out) -> in.receive().then()).bindNow();

        this.client = ReactorNettyClient.connect(InetSocketAddress.createUnresolved(this.server.host(), this.server.port()), ConnectionSettings.builder().build()).block();

        MockCodecs.Builder codecs = MockCodecs.builder();
        for (int i = 0; i < ROWS; i++) {
            codecs = codecs.encoding("row-" + i, lazyParameter("row-" + i));
        }

        this.resources = MockContext.builder().client(this.client).codecs(codecs.build()).build();

        when(this.resources.getStatementCache().getName(any(), any())).thenReturn("test-name");
        when(this.resources.getStatementCache().requiresPrepare(any(), any())).thenReturn(false);
    }

    @AfterEach
    void after() {

        closeClient();

        if (this.server != null) {
            this.server.disposeNow();
        }

        for (ByteBuf parameter : this.parameters) {
            if (parameter.refCnt() > 0) {
                parameter.release(parameter.refCnt());
            }
        }
        this.parameters.clear();
    }

    /**
     * Control: the very same cancelled workload releases its bind parameters once the statement made it onto the wire, because {@link Bind#encode(ByteBuf)} disposes them.
     */
    @Test
    void bindParametersAreReleasedWhenTheCancelledStatementReachedTheWire() {

        reporterShape()
            .as(StepVerifier::create)
            .thenAwait(Duration.ofMillis(250))
            .thenCancel()
            .verify(Duration.ofSeconds(10));

        closeClient();

        assertThat(this.parameters).describedAs("the statement must have encoded its parameter").hasSize(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> assertThat(this.parameters.get(0).refCnt()).describedAs("bind parameter buffer of a statement that reached the wire").isZero());
    }

    /**
     * The connection is gone before the statement is subscribed, so the {@link Bind} is built (retaining the parameter buffers) but is never encoded.
     */
    @Test
    void bindParametersAreReleasedWhenTheStatementIsRejectedByAClosedConnection() {

        getConnection().channel().close().awaitUninterruptibly();

        reporterShape()
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessageContaining("Cannot exchange messages because the connection is closed"));

        closeClient();

        assertThat(this.parameters).describedAs("the statement must have encoded its parameter").hasSize(1);

        Awaitility.await().atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> assertThat(this.parameters.get(0).refCnt()).describedAs("bind parameter buffer of a statement that never reached the wire").isZero());
    }

    /**
     * The statement is accepted while the connection is alive and queued in the client's outbound pipeline. The channel dies before the queued message is flushed, so
     * {@code ChannelOperations#send(Publisher, Predicate)} aborts without ever subscribing the cold encode {@link Mono}.
     */
    @Test
    void bindParametersAreReleasedWhenTheChannelDiesWhileTheStatementIsQueued() throws Exception {

        Connection connection = getConnection();

        // Occupy the client's outbound pipeline with a conversation whose request publisher never completes so that the
        // statement below is queued inside ReactorNettyClient instead of being flushed straight away.
        Sinks.Many<FrontendMessage> blocker = Sinks.many().unicast().onBackpressureBuffer();
        Disposable blocking = this.client.exchange(message -> false, blocker.asFlux()).subscribe(message -> {
        }, throwable -> {
        });

        AtomicReference<Throwable> error = new AtomicReference<>();
        Disposable work = reporterShape().subscribe(unused -> {
        }, error::set);

        assertThat(error.get()).describedAs("the statement must have been accepted while the connection was alive").isNull();
        assertThat(this.parameters).describedAs("the statement must have encoded its parameter").hasSize(1);

        connection.channel().eventLoop().submit(() -> {
            connection.channel().close();
            blocker.tryEmitComplete();
        }).await(5, TimeUnit.SECONDS);

        closeClient();

        try {
            Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(this.parameters.get(0).refCnt()).describedAs("bind parameter buffer of a statement discarded before it was flushed").isZero());
        } finally {
            work.dispose();
            blocking.dispose();
        }
    }

    /**
     * The workload reported in gh-734: a batch of parameterized inserts, run one after another, that is cancelled while it is still in flight.
     */
    private Mono<Void> reporterShape() {

        return Flux.range(0, ROWS)
            .concatMap(i -> Mono.from(new PostgresqlStatement(this.resources, SQL).bind("$1", "row-" + i).execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated())))
            .then();
    }

    private void closeClient() {

        if (this.client != null) {
            this.client.close().onErrorResume(e -> Mono.empty()).block(Duration.ofSeconds(5));
        }
    }

    private Connection getConnection() {
        return (Connection) ReflectionUtils.getField(CONNECTION, this.client);
    }

    private EncodedParameter lazyParameter(String value) {

        return new EncodedParameter(FORMAT_TEXT, VARCHAR.getObjectId(), Flux.defer(() -> {

            ByteBuf buffer = TestByteBufAllocator.TEST.buffer();
            buffer.writeCharSequence(value, StandardCharsets.UTF_8);
            this.parameters.add(buffer);

            return Flux.just(buffer);
        }));
    }

}