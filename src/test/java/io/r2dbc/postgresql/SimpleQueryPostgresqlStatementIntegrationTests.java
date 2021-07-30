/*
 * Copyright 2021 the original author or authors.
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

/**
 * Integration tests for {@link SimpleQueryPostgresqlStatement}.
 */
class SimpleQueryPostgresqlStatementIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void setUp() {
        super.setUp();
        getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        getJdbcOperations().execute("CREATE TABLE test (val VARCHAR(255))");
        getJdbcOperations().execute("INSERT INTO test (val) VALUES ('a'), ('b')");
    }

    @AfterEach
    void tearDown() {
        super.tearDown();
        getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }

    private JdbcOperations getJdbcOperations() {
        return SERVER.getJdbcOperations();
    }

    @Test
    void shouldRunMultipleQueries() {

        this.connection.createStatement("SELECT 1;SELECT val FROM test")
            .fetchSize(0).execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Tuples.of(row.get(0), rowMetadata.getColumnMetadata(0).getName())))
            .as(StepVerifier::create)
            .expectNext(Tuples.of(1, "?column?"), Tuples.of("a", "val"), Tuples.of("b", "val"))
            .verifyComplete();
    }

}
