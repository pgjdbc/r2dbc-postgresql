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

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.AbstractIntegrationTests;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PostgresTypes}.
 */
class PostgresTypesIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldLookupSingleType() {

        Flux
            .usingWhen( //
                getConnectionFactory().create(), //
                c -> PostgresTypes.from(c).lookupTypes("varchar"), //
                Connection::close //
            )
            .map(PostgresTypes.PostgresType::getName)
            .map(String::toLowerCase)
            .as(StepVerifier::create)
            .expectNext("varchar")
            .verifyComplete();
    }

    @Test
    void shouldLookupTypesInDifferentSchemas() {

        // test enum type set up
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        jdbcOperations.execute("CREATE SCHEMA test_schema_1;");
        jdbcOperations.execute("CREATE SCHEMA test_schema_2;");
        jdbcOperations.execute("CREATE TYPE test_schema_1.test_enum AS ENUM ('FIRST', 'SECOND');");
        jdbcOperations.execute("CREATE TYPE test_schema_2.test_enum AS ENUM ('FIRST', 'SECOND');");

        Flux
            .usingWhen( //
                getConnectionFactory().create(), //
                c -> c //
                    .createStatement("SET SEARCH_PATH TO test_schema_1, test_schema_2;") //
                    .execute() //
                    .flatMap(unused -> PostgresTypes.from(c).lookupTypes("test_enum")), //
                Connection::close //
            )
            .as(StepVerifier::create)
            .expectNextMatches(type -> type.getName().equals("test_enum"))
            .expectNextMatches(type -> type.getName().equals("test_enum"))
            .verifyComplete();
    }

    @Test
    void shouldLookupMultipleType() {

        Flux.usingWhen(getConnectionFactory().create(), c -> {
            return PostgresTypes.from(c).lookupTypes(Arrays.asList("varchar", "int4"));
        }, Connection::close).map(PostgresTypes.PostgresType::getName).map(String::toLowerCase).collectList()
            .as(StepVerifier::create).consumeNextWith(actual -> {
            assertThat(actual).contains("varchar", "int4");
        }).verifyComplete();
    }

}
