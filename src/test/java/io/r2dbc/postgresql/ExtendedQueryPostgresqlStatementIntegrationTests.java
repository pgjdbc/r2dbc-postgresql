/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link ExtendedQueryPostgresqlStatement}
 */
final class ExtendedQueryPostgresqlStatementIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void setUp() {
        super.setUp();
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY, val VARCHAR(255))");
        SERVER.getJdbcOperations().execute("INSERT INTO test (val) VALUES ('a'), ('a'), ('b'), ('c'), ('c')");
    }

    @AfterEach
    void tearDown() {
        super.tearDown();
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.preparedStatementCacheQueries(2);
    }

    @Test
    void shouldRunMultipleQueries() {

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(0)
            .bind("$1", "a").add()
            .bind("$1", "b").add()
            .bind("$1", "c").execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a", "b", "c", "c")
            .verifyComplete();
    }

    @Test
    void shouldRunMultipleQueriesWithFetchSize() {

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(1)
            .bind("$1", "a").add()
            .bind("$1", "b").add()
            .bind("$1", "c").execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a", "b", "c", "c")
            .verifyComplete();
    }

    @Test
    void shouldRecoverFromWrongBinding() {

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(0)
            .bind("$1", "a").add()
            .bind("$1", 1).add()
            .bind("$1", "c").execute())
            .concatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a")
            .expectError(R2dbcBadGrammarException.class)
            .verify();

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .bind("$1", "a").execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a")
            .verifyComplete();
    }

    @Test
    void shouldRecoverFromWrongBindingWithFetchSize() {

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(1)
            .bind("$1", "a").add()
            .bind("$1", 1).add()
            .bind("$1", "c").execute())
            .concatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a")
            .expectError(R2dbcBadGrammarException.class)
            .verify();

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .bind("$1", "a").execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a")
            .verifyComplete();
    }

    @Test
    void shouldRunSingleQueryInTxWithFetchSize() {

        this.connection.beginTransaction().block();

        try {
            Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
                .fetchSize(1)
                .bind("$1", "a").execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
                .as(StepVerifier::create)
                .expectNext("a", "a")
                .verifyComplete();
        } finally {
            this.connection.rollbackTransaction().block();
        }
    }

    @Test
    void shouldRunMultipleQueriesInTxWithFetchSize() {

        this.connection.beginTransaction().block();

        try {
            Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
                .fetchSize(1)
                .bind("$1", "a").add()
                .bind("$1", "b").execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
                .as(StepVerifier::create)
                .expectNext("a", "a")
                .expectNext("b")
                .verifyComplete();
        } finally {
            this.connection.rollbackTransaction().block();
        }
    }

    @Test
    void shouldNotRecoverFromWrongBindingInTx() {

        this.connection.beginTransaction().block();

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(0)
            .bind("$1", "a").add()
            .bind("$1", 1).add()
            .bind("$1", "c").execute())
            .concatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .expectNext("a", "a")
            .expectError(R2dbcBadGrammarException.class)
            .verify();

        Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
            .fetchSize(0)
            .bind("$1", "a").execute())
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);

        this.connection.rollbackTransaction().block();
    }

    @Test
    void shouldUnprepareStatements() {

        this.connection.beginTransaction().block();

        try {
            Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1 AND 1=1")
                .fetchSize(1)
                .bind("$1", "a").add()
                .bind("$1", "b").execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
                .as(StepVerifier::create)
                .expectNext("a", "a")
                .expectNext("b")
                .verifyComplete();

            Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1 AND 2=2")
                .fetchSize(1)
                .bind("$1", "a").add()
                .bind("$1", "b").execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
                .as(StepVerifier::create)
                .expectNext("a", "a")
                .expectNext("b")
                .verifyComplete();

            Flux.from(this.connection.createStatement("SELECT * FROM test WHERE val = $1")
                .fetchSize(1)
                .bind("$1", "a").add()
                .bind("$1", "b").execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(1)))
                .as(StepVerifier::create)
                .expectNext("a", "a")
                .expectNext("b")
                .verifyComplete();
        } finally {
            this.connection.rollbackTransaction().block();
        }
    }

}
