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

import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

final class PostgresqlStatementErrorsIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, POSTGRESQL_DRIVER)
        .option(DATABASE, SERVER.getDatabase())
        .option(HOST, SERVER.getHost())
        .option(PORT, SERVER.getPort())
        .option(PASSWORD, SERVER.getPassword())
        .option(USER, SERVER.getUsername())
        .build());

    Connection connection;

    @BeforeEach
    void setUp() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        connection = Mono.from(connectionFactory.create()).block();
    }


    @AfterEach
    void tearDown() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        Mono.from(connection.close()).block();
    }

    @Test
    void shouldReportDataIntegrityViolationUsingSimpleFlow() {

        SERVER.getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY)");

        Flux<Integer> insert = Flux.from(connection.createStatement("INSERT INTO test (id) VALUES (1) RETURNING *").execute()).flatMap(Result::getRowsUpdated);

        insert.thenMany(insert).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);
    }

    @Test
    void shouldReportDataIntegrityViolationUsingExtendedFlow() {

        SERVER.getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY)");

        Flux<Integer> insert = Flux.from(connection.createStatement("INSERT INTO test (id) VALUES ($1) RETURNING *").bind("$1", 1).execute()).flatMap(Result::getRowsUpdated);

        insert.thenMany(insert).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);
    }

}
