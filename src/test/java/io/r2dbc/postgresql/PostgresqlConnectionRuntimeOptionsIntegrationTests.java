package io.r2dbc.postgresql;

import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests for {@link PostgresqlConnection} connection options.
 */
final class PostgresqlConnectionRuntimeOptionsIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    private static final Map<String, String> options = new HashMap<>();

    static {
        options.put("lock_timeout", "5000");
        options.put("statement_timeout", "60s");
    }

    private final PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
        .database(SERVER.getDatabase())
        .host(SERVER.getHost())
        .port(SERVER.getPort())
        .password(SERVER.getPassword())
        .username(SERVER.getUsername())
        .forceBinary(true)
        .options(options)
        .build();

    private final PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(this.configuration);

    @Test
    void connectionFactoryShouldApplyParameters() {
        PostgresqlConnection connection = (PostgresqlConnection) connectionFactory.create().block();

        connection
            .createStatement("SHOW lock_timeout").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("lock_timeout", String.class)))
            .as(StepVerifier::create)
            .expectNext("5s")
            .verifyComplete();

        connection
            .createStatement("SHOW statement_timeout").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("statement_timeout", String.class)))
            .as(StepVerifier::create)
            .expectNext("1min")
            .verifyComplete();

        connection.close().block();
    }

    @Test
    void connectionFactoryShouldApplyParametersUsingTimeoutApis() {
        PostgresqlConnection connection = (PostgresqlConnection) connectionFactory.create().block();
        connection.lockTimeout(Duration.ofSeconds(10)).block();
        connection.statementTimeout(Duration.ofMinutes(2)).block();

        connection
            .createStatement("SHOW lock_timeout").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("lock_timeout", String.class)))
            .as(StepVerifier::create)
            .expectNext("10s")
            .verifyComplete();

        connection
            .createStatement("SHOW statement_timeout").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("statement_timeout", String.class)))
            .as(StepVerifier::create)
            .expectNext("2min")
            .verifyComplete();

        connection.close().block();
    }

}
