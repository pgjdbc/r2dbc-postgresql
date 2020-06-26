/*
 * Copyright 2020 the original author or authors.
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

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.ErrorDetails;
import io.r2dbc.postgresql.api.PostgresqlException;
import io.r2dbc.postgresql.util.PgBouncer;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.R2dbcBadGrammarException;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link PostgresqlConnectionFactory} through PG Bouncer.
 */
final class PgBouncerIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    @ParameterizedTest
    @ValueSource(strings = {"transaction", "statement"})
    void disabledCacheWorksWithTransactionAndStatementModes(String poolMode) {
        try (PgBouncer pgBouncer = new PgBouncer(SERVER, poolMode)) {
            PostgresqlConnectionFactory connectionFactory = this.createConnectionFactory(pgBouncer, 0);

            connectionFactory.create().flatMapMany(connection -> {
                Flux<Integer> q1 = connection.createStatement("SELECT 1 WHERE $1 = 1").bind(0, 1).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));
                Flux<Integer> q2 = connection.createStatement("SELECT 2 WHERE $1 = 2").bind(0, 2).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));
                Flux<Integer> q3 = connection.createStatement("SELECT 3 WHERE $1 = 3").bind(0, 3).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));

                return Flux.concat(q1, q1, q2, q2, q3, q3, connection.close());
            })
                .as(StepVerifier::create)
                .expectNext(1, 1, 2, 2, 3, 3)
                .verifyComplete();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 2})
    void sessionModeWorksWithAllCaches(int statementCacheSize) {
        try (PgBouncer pgBouncer = new PgBouncer(SERVER, "session")) {
            PostgresqlConnectionFactory connectionFactory = this.createConnectionFactory(pgBouncer, statementCacheSize);

            connectionFactory.create().flatMapMany(connection -> {
                Flux<Integer> q1 = connection.createStatement("SELECT 1 WHERE $1 = 1").bind(0, 1).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));
                Flux<Integer> q2 = connection.createStatement("SELECT 2 WHERE $1 = 2").bind(0, 2).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));
                Flux<Integer> q3 = connection.createStatement("SELECT 3 WHERE $1 = 3").bind(0, 3).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));

                return Flux.concat(q1, q1, q2, q2, q3, q3, connection.close());
            })
                .as(StepVerifier::create)
                .expectNext(1, 1, 2, 2, 3, 3)
                .verifyComplete();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"transaction", "statement"})
    void statementCacheDoesntWorkWithTransactionAndStatementModes(String poolMode) {
        try (PgBouncer pgBouncer = new PgBouncer(SERVER, poolMode)) {
            PostgresqlConnectionFactory connectionFactory = this.createConnectionFactory(pgBouncer, -1);

            connectionFactory.create().flatMapMany(connection -> {
                Flux<Integer> q1 = connection.createStatement("SELECT 1 WHERE $1 = 1").bind(0, 1).execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0, Integer.class)));

                return Flux.concat(q1, q1, connection.close());
            })
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyErrorMatches(e -> {
                    if (!(e instanceof R2dbcBadGrammarException)) {
                        return false;
                    }
                    if (!(e instanceof PostgresqlException)) {
                        return false;
                    }
                    PostgresqlException pgException = (PostgresqlException) e;
                    ErrorDetails errorDetails = pgException.getErrorDetails();
                    return errorDetails.getCode().equals("26000") && errorDetails.getMessage().equals("prepared statement \"S_0\" does not exist");
                });
        }
    }

    private PostgresqlConnectionFactory createConnectionFactory(PgBouncer pgBouncer, int statementCacheSize) {
        return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host(pgBouncer.getHost())
            .port(pgBouncer.getPort())
            .username(SERVER.getUsername())
            .password(SERVER.getPassword())
            .database(SERVER.getDatabase())
            .preparedStatementCacheQueries(statementCacheSize)
            .applicationName(getClass().getName())
            .build());
    }
}
