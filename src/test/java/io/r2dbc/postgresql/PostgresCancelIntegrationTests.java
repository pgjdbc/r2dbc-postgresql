/*
 * Copyright 2020-2020 the original author or authors.
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for cancellation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresCancelIntegrationTests extends AbstractIntegrationTests {

    static final int NUMBER_REPETITIONS = 300;

    @Override
    @BeforeAll
    void setUp() {

        super.setUp();

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS insert_test;");
        SERVER.getJdbcOperations().execute("CREATE TABLE insert_test\n" +
            "(\n" +
            "    id    SERIAL PRIMARY KEY,\n" +
            "    value CHAR(1) NOT NULL\n" +
            ");");
    }

    @AfterAll
    void tearDown() {
        super.tearDown();
    }

    @Test
    void shouldBeginAndCommitCancel() throws InterruptedException {

        // precondition
        assertThat(this.connection.isAutoCommit()).isTrue();

        this.connection.beginTransaction().then(this.connection.commitTransaction())
            .as(StepVerifier::create)
            .thenCancel()
            .verify();


        // await completion
        Thread.sleep(100);

        assertThat(this.connection.isAutoCommit()).isTrue();
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
}
