/*
 * Copyright 2022 the original author or authors.
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
import org.junit.jupiter.api.Test;
import reactor.netty.DisposableChannel;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class DowntimeIntegrationTests {

    @Test
    void failSslHandshakeIfInboundClosed() {
        // Simulate server downtime, where connections are accepted and then closed immediately
        DisposableServer server =
            TcpServer.create()
                .doOnConnection(DisposableChannel::dispose)
                .bindNow();

        PostgresqlConnectionFactory connectionFactory =
            new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder()
                    .host(server.host())
                    .port(server.port())
                    .username("test")
                    .sslMode(SSLMode.REQUIRE)
                    .build());

        connectionFactory.create()
            .as(StepVerifier::create)
            .verifyErrorSatisfies(error ->
                assertThat(error)
                    .isInstanceOf(AbstractPostgresSSLHandlerAdapter.PostgresqlSslException.class)
                    .hasMessage("Connection closed during SSL negotiation"));

        server.disposeNow();
    }

}
