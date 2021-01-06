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

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.AbstractIntegrationTests;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link EnumCodec}.
 */
final class EnumCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldReportUnresolvableTypes() {

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .database(SERVER.getDatabase())
            .host(SERVER.getHost())
            .port(SERVER.getPort())
            .password(SERVER.getPassword())
            .username(SERVER.getUsername())
            .codecRegistrar(EnumCodec.builder().withEnum("do_not_exist", MyEnum.class).build())
            .build();

        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(configuration);
        connectionFactory.create().flatMap(PostgresqlConnection::close).as(StepVerifier::create).verifyComplete();

        // we cannot really assert logs so that's up to you.
    }

    @Test
    void shouldBindEnumType() {

        try {
            SERVER.getJdbcOperations().execute("CREATE TYPE my_enum AS ENUM ('HELLO', 'WORLD')");
        } catch (DataAccessException e) {
            // ignore duplicate types
        }

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS enum_test");
        SERVER.getJdbcOperations().execute("CREATE TABLE enum_test (the_value my_enum);");

        PostgresTypes types = PostgresTypes.from(this.connection);
        PostgresTypes.PostgresType type = types.lookupType("my_enum").block();

        this.connection.createStatement("INSERT INTO enum_test VALUES($1)")
            .bind("$1", Parameters.in(type, "HELLO"))
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        String result = SERVER.getJdbcOperations().queryForObject("SELECT the_value FROM enum_test", String.class);
        assertThat(result).isEqualTo("HELLO");
    }

    enum MyEnum {
        HELLO;
    }

}
