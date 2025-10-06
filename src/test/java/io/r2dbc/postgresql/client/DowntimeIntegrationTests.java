/*
 * Copyright 2023 the original author or authors.
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

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlException;
import io.r2dbc.postgresql.util.Disposable;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class DowntimeIntegrationTests {

    @Test
    void failSslHandshakeIfInboundClosed(@Disposable InetSocketAddress faultyServer) {
        verifyError(faultyServer, SSLMode.REQUIRE, error ->
            assertThat(error)
                .isInstanceOf(AbstractPostgresSSLHandlerAdapter.PostgresqlSslException.class)
                .hasMessage("Connection closed during SSL negotiation"));
    }

    @Test
    void failSslTunnelIfInboundClosed(@Disposable InetSocketAddress faultyServer) {
        verifyError(faultyServer, SSLMode.TUNNEL, error -> {
            assertThat(error)
                .isInstanceOf(PostgresqlException.class)
                .cause()
                .isInstanceOf(ClosedChannelException.class);

            assertThat(error.getCause().getSuppressed()).hasSize(1);

            assertThat(error.getCause().getSuppressed()[0])
                .hasMessage("Connection closed while SSL/TLS handshake was in progress");
        });
    }

    static PostgresqlConnectionFactory newConnectionFactory(InetSocketAddress server, SSLMode sslMode) {
        return new PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(server.getHostString())
                .port(server.getPort())
                .username("test")
                .sslMode(sslMode)
                .build());
    }

    static void verifyError(InetSocketAddress server, SSLMode sslMode, Consumer<Throwable> assertions) {
        PostgresqlConnectionFactory connectionFactory = newConnectionFactory(server, sslMode);
        connectionFactory.create().as(StepVerifier::create).verifyErrorSatisfies(assertions);
    }

}
