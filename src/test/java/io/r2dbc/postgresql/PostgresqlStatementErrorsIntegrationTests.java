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

import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

final class PostgresqlStatementErrorsIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void setUp() {
        super.setUp();
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }


    @AfterEach
    void tearDown() {
        super.tearDown();
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }

    @Test
    void shouldReportDataIntegrityViolationUsingSimpleFlow() {

        SERVER.getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY)");

        Flux<Integer> insert = Flux.from(this.connection.createStatement("INSERT INTO test (id) VALUES (1) RETURNING *").execute()).flatMap(Result::getRowsUpdated);

        insert.thenMany(insert).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);
    }

    @Test
    void shouldReportDataIntegrityViolationUsingExtendedFlow() {

        SERVER.getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY)");

        Flux<Integer> insert = Flux.from(this.connection.createStatement("INSERT INTO test (id) VALUES ($1) RETURNING *").bind("$1", 1).execute()).flatMap(Result::getRowsUpdated);

        insert.thenMany(insert).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);
    }

}
