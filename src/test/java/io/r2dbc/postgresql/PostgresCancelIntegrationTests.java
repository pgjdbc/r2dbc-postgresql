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

import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Integration tests for cancellation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class PostgresCancelIntegrationTests extends AbstractIntegrationTests {

    static final int NUMBER_REPETITIONS = 300;

    @Override
    @BeforeAll
    void setUp() {

        super.setUp();

        JdbcOperations jdbc = SERVER.getJdbcOperations();
        jdbc.execute("DROP TABLE IF EXISTS insert_test;");
        jdbc.execute("CREATE TABLE insert_test\n" +
            "(\n" +
            "    id    SERIAL PRIMARY KEY,\n" +
            "    value CHAR(1) NOT NULL\n" +
            ");");


        jdbc.execute("DROP TABLE IF EXISTS lots_of_data;");
        jdbc.execute("CREATE TABLE lots_of_data AS \n"
            + "  SELECT i FROM generate_series(1,200000) as i;");
    }

    @AfterAll
    void tearDown() {
        super.tearDown();
    }

    @RepeatedTest(NUMBER_REPETITIONS)
    void shouldCancelSimpleQuery() {

        this.connection.createStatement("INSERT INTO insert_test (value) VALUES('a')")
            .returnGeneratedValues()
            .execute().flatMap(postgresqlResult -> postgresqlResult.map((row, rowMetadata) -> row.get(0)), 1, 1)
            .next()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @RepeatedTest(NUMBER_REPETITIONS)
    void shouldCancelParametrizedQuery() {

        this.connection.createStatement("INSERT INTO insert_test (value) VALUES($1)")
            .returnGeneratedValues()
            .bind("$1", "a")
            .execute().flatMap(postgresqlResult -> postgresqlResult.map((row, rowMetadata) -> row.get(0)), 1, 1)
            .next()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @RepeatedTest(NUMBER_REPETITIONS)
    void shouldCancelParametrizedWithMultipleBindingsQuery() {

        this.connection.createStatement("INSERT INTO insert_test (value) VALUES($1)")
            .returnGeneratedValues()
            .bind("$1", "a").add()
            .bind("$1", "b")
            .execute().flatMap(postgresqlResult -> postgresqlResult.map((row, rowMetadata) -> row.get(0)), 1, 1)
            .next()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @Test
    void cancelRequest() {
        Mono<Void> cancel = this.connection.cancelRequest()
            .delaySubscription(Duration.ofSeconds(1));

        this.connection.createStatement("SELECT pg_sleep(1000)")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .mergeWith(cancel.then(Mono.empty()))
            .as(StepVerifier::create)
            .expectErrorMatches(e -> e instanceof R2dbcNonTransientResourceException && e.getMessage().equals("canceling statement due to user request"))
            .verify(Duration.ofSeconds(5));
    }

    @Timeout(10)
    @RepeatedTest(20)
    void shouldCancelParametrizedWithFetchSize() {

        this.connection.createStatement("SELECT * FROM lots_of_data WHERE $1 = $1 ORDER BY i")
            .fetchSize(10)
            .bind(0, 1)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(1)
            .expectNextCount(5)
            .thenCancel()
            .verify(Duration.ofSeconds(5));
    }

    @Timeout(10)
    @RepeatedTest(20)
    void shouldCancelSimpleWithFetchSize() {

        this.connection.createStatement("SELECT * FROM lots_of_data ORDER BY i")
            .fetchSize(10)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(1)
            .expectNextCount(5)
            .thenCancel()
            .verify(Duration.ofSeconds(5));
    }
}
