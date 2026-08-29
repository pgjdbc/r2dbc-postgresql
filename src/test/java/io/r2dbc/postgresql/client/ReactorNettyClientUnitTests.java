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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReactorNettyClient}.
 */
final class ReactorNettyClientUnitTests {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final Field CONNECTION = ReflectionUtils.findField(ReactorNettyClient.class, "connection");

    static {
        ReflectionUtils.makeAccessible(CONNECTION);
    }

    private DisposableServer server;

    private ReactorNettyClient client;

    @BeforeEach
    void before() {

        this.server = TcpServer.create().host("localhost").port(0).handle((in, out) -> out.neverComplete()).bindNow();
        this.client = ReactorNettyClient.connect(InetSocketAddress.createUnresolved(this.server.host(), this.server.port()), ConnectionSettings.builder().build()).block(TIMEOUT);
    }

    @AfterEach
    void after() {

        if (this.client != null) {
            this.client.close().onErrorResume(e -> Mono.empty()).block(TIMEOUT);
        }

        if (this.server != null) {
            this.server.disposeNow();
        }
    }

    @Test
    void shouldReleaseBindParametersWhenMessageIsWritten() {

        ByteBuf parameter = TEST.buffer(4).writeInt(42);
        Bind bind = new Bind("", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(parameter), Collections.singletonList(FORMAT_BINARY), "");

        Sinks.Many<FrontendMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        CompletableFuture<List<BackendMessage>> exchange = this.client.exchange(message -> false, messages.asFlux()).collectList().toFuture();

        messages.tryEmitNext(bind);

        Awaitility.await().atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(parameter.refCnt()).describedAs("Bind parameter must be released once the message got encoded").isZero());

        exchange.cancel(true);
    }

    @Test
    void shouldReleaseBindParametersWhenChannelIsInactiveBeforeMessageIsWritten() throws Exception {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);

        ByteBuf parameter = TEST.buffer(4).writeInt(42);
        Bind bind = new Bind("", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(parameter), Collections.singletonList(FORMAT_BINARY), "");

        Sinks.Many<FrontendMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        CompletableFuture<List<BackendMessage>> exchange = this.client.exchange(message -> false, messages.asFlux()).collectList().toFuture();

        // Close the channel and hand over the message within the same event loop task. The channel is no longer active
        // while the request sink has not been completed yet so the message reaches the outbound chain but never the wire.
        connection.channel().eventLoop().execute(() -> {

            connection.channel().close();
            messages.tryEmitNext(bind);
        });

        Throwable error = exchange.handle((result, throwable) -> throwable).get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertThat(error).describedAs("Exchange must terminate because the connection got closed").isNotNull();

        Awaitility.await().atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(parameter.refCnt()).describedAs("Bind parameter must be released although the message never reached the wire").isZero());
    }

}