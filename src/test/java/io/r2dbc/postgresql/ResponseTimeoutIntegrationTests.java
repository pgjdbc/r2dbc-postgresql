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

import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises public configuration and protocol boundaries against a real PostgreSQL server.
 */
@Timeout(20)
final class ResponseTimeoutIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    private static final Duration LIMIT = Duration.ofSeconds(8);

    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(1);

    @Test
    void publicBuilderTimesOutQuietStatementAndInvalidatesConnection() {
        PostgresqlConnection connection = connect();
        try {
            StepVerifier.create(execute(connection, "SELECT pg_sleep(3)"))
                .expectErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(R2dbcNonTransientResourceException.class);
                    assertThat(((R2dbcNonTransientResourceException) e).getSqlState()).isEqualTo("08006");
                }).verify(LIMIT);
            assertThat(connection.validate(ValidationDepth.LOCAL).block(LIMIT)).isFalse();
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void urlOptionReachesClient() {
        String url = String.format("r2dbc:postgresql://%s:%s@%s:%d/%s?responseTimeout=PT1S",
            SERVER.getUsername(), SERVER.getPassword(), SERVER.getHost(), SERVER.getPort(), SERVER.getDatabase());
        PostgresqlConnection connection = new PostgresqlConnectionFactoryProvider().create(ConnectionFactoryOptions.parse(url)).create().block(LIMIT);
        try {
            StepVerifier.create(execute(connection, "SELECT pg_sleep(3)"))
                .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void idleAndListenConnectionRemainUsable() {
        PostgresqlConnection connection = connect();
        try {
            execute(connection, "LISTEN response_timeout_test").block(LIMIT);
            Mono.delay(RESPONSE_TIMEOUT.multipliedBy(2)).block(LIMIT);
            assertThat(connection.validate(ValidationDepth.REMOTE).block(LIMIT)).isTrue();
            execute(connection, "UNLISTEN *").block(LIMIT);
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void disabledTimeoutAllowsLongStatement() {
        PostgresqlConnection connection = new PostgresqlConnectionFactory(SERVER.configBuilder().responseTimeout(Duration.ZERO).build()).create().block(LIMIT);
        try {
            execute(connection, "SELECT pg_sleep(2)").block(LIMIT);
            assertThat(connection.validate(ValidationDepth.REMOTE).block(LIMIT)).isTrue();
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void cursorAndSlowConsumerCanOutliveTimeout() {
        PostgresqlConnection connection = connect();
        try {
            StepVerifier.create(connection.createStatement("SELECT i, pg_sleep(0.1) FROM generate_series(1, 15) i").fetchSize(1)
                    .execute().flatMap(result -> result.map((row, metadata) -> row.get(0, Integer.class)))
                    .delayElements(Duration.ofMillis(100)))
                .expectNextCount(15).expectComplete().verify(LIMIT);
            assertThat(connection.validate(ValidationDepth.REMOTE).block(LIMIT)).isTrue();
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void copyInputMayPauseLongerThanResponseTimeout() {
        PostgresqlConnection connection = connect();
        try {
            execute(connection, "CREATE TEMP TABLE response_timeout_copy (value integer)").block(LIMIT);
            Mono<io.netty.buffer.ByteBuf> input = Mono.delay(RESPONSE_TIMEOUT.multipliedBy(2))
                .map(it -> Unpooled.copiedBuffer("42\n", StandardCharsets.UTF_8));
            assertThat(connection.copyIn("COPY response_timeout_copy FROM STDIN", input).block(LIMIT)).isEqualTo(1L);
            assertThat(connection.validate(ValidationDepth.REMOTE).block(LIMIT)).isTrue();
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void immediateCopyInputDoesNotLeaveWatchdogPaused() {
        PostgresqlConnection connection = connect();
        try {
            execute(connection, "CREATE TEMP TABLE response_timeout_copy (value integer)").block(LIMIT);
            assertThat(connection.copyIn("COPY response_timeout_copy FROM STDIN",
                Mono.fromSupplier(() -> Unpooled.copiedBuffer("42\n", StandardCharsets.UTF_8))).block(LIMIT)).isEqualTo(1L);
            StepVerifier.create(execute(connection, "SELECT pg_sleep(3)"))
                .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
        } finally {
            connection.close().block(LIMIT);
        }
    }

    @Test
    void sslConnectionUsesSameResponseTimeout() {
        PostgresqlConnection connection = new PostgresqlConnectionFactory(SERVER.configBuilder().username("test-ssl").password("test-ssl").sslMode(SSLMode.REQUIRE)
            .responseTimeout(RESPONSE_TIMEOUT).build()).create().block(LIMIT);
        try {
            execute(connection, "SELECT 1").block(LIMIT);
            StepVerifier.create(execute(connection, "SELECT pg_sleep(3)"))
                .expectError(R2dbcNonTransientResourceException.class).verify(LIMIT);
        } finally {
            connection.close().block(LIMIT);
        }
    }

    private PostgresqlConnection connect() {
        return new PostgresqlConnectionFactory(SERVER.configBuilder().responseTimeout(RESPONSE_TIMEOUT).build()).create().block(LIMIT);
    }

    private Mono<Void> execute(PostgresqlConnection connection, String sql) {
        return connection.createStatement(sql).execute().flatMap(result -> Flux.from(result.getRowsUpdated())).then();
    }

}
