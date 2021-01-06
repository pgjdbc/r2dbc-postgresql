/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.codec.Json;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link Json}.
 */
final class JsonIntegrationTests extends AbstractIntegrationTests{

    @Test
    void shouldReadAndWriteJson() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        jdbcOperations.execute("DROP TABLE IF EXISTS my_table;");
        jdbcOperations.execute("CREATE TABLE my_table (my_json JSON)");

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("INSERT INTO my_table (my_json) VALUES($1)")
            .bind("$1", Json.of("{\"hello\": \"world\"}")).execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        connection.createStatement("SELECT my_json FROM my_table")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_json", Json.class)))
            .map(Json::asString)
            .as(StepVerifier::create)
            .expectNext("{\"hello\": \"world\"}")
            .verifyComplete();

        connection.close().block();
    }

    @Test
    void shouldReadAndWriteAsString() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        jdbcOperations.execute("DROP TABLE IF EXISTS my_table;");
        jdbcOperations.execute("CREATE TABLE my_table (my_json JSON)");

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("INSERT INTO my_table (my_json) VALUES($1::JSON)")
            .bind("$1", "{\"hello\": \"world\"}").execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        connection.createStatement("SELECT my_json FROM my_table")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_json", String.class)))
            .as(StepVerifier::create)
            .expectNext("{\"hello\": \"world\"}")
            .verifyComplete();

        connection.close().block();
    }

}
