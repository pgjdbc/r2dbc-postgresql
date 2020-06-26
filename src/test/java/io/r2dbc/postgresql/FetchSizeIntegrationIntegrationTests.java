/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link Statement#fetchSize(int)}.
 */
final class FetchSizeIntegrationIntegrationTests extends AbstractIntegrationTests {

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.fetchSize(100);
    }

    @Test
    void exchangeWithDefaultFetchSize() {
        this.connection.createStatement("SELECT * FROM generate_series(1,20) WHERE $1 = $1")
            .bind(0, 1)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNextCount(20)
            .verifyComplete();
    }

    @Test
    void exchangeWithFetchSize() {
        this.connection.createStatement("SELECT * FROM generate_series(1,20) WHERE $1 = $1")
            .bind(0, 1)
            .fetchSize(11)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNextCount(20)
            .verifyComplete();
    }

    @Test
    void exchangeWithCancel() {
        this.connection.createStatement("SELECT * FROM generate_series(1,200) WHERE $1 = $1")
            .fetchSize(6)
            .bind(0, 1)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .take(20)
            .as(StepVerifier::create)
            .expectNextCount(20)
            .verifyComplete();
    }

    @Test
    void exchangeWithLimitRequest() {
        this.connection.createStatement("SELECT * FROM generate_series(1,200) WHERE $1 = $1")
            .fetchSize(6)
            .bind(0, 1)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .limitRequest(20)
            .as(StepVerifier::create)
            .expectNextCount(20)
            .verifyComplete();
    }

    @Test
    void simpleQueryWithFetchSize() {
        this.connection.createBatch()
            .add("TEST")
            .execute();
        this.connection.createStatement("SELECT * FROM generate_series(1,200)")
            .fetchSize(6)
            .execute()
            .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
            .limitRequest(20)
            .as(StepVerifier::create)
            .expectNextCount(20)
            .verifyComplete();
    }

}
