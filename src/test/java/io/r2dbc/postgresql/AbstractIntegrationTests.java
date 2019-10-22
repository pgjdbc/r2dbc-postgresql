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
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

/**
 * Support class for integration tests using {@link PostgresqlConnection}.
 */
abstract class AbstractIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    PostgresqlConnectionFactory connectionFactory;

    PostgresqlConnection connection;

    /**
     * Entry-point to obtain a {@link PostgresqlConnectionFactory}.
     *
     * @return a {@link PostgresqlConnectionFactory}.
     */
    protected PostgresqlConnectionFactory getConnectionFactory() {

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder()
            .database(SERVER.getDatabase())
            .host(SERVER.getHost())
            .port(SERVER.getPort())
            .password(SERVER.getPassword())
            .username(SERVER.getUsername());

        customize(builder);
        return new PostgresqlConnectionFactory(builder.build());
    }

    /**
     * Template method to customize {@link PostgresqlConnectionConfiguration.Builder}.
     *
     * @param builder builder to customize.
     */
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {

    }

    @BeforeEach
    void setUp() {
        this.connectionFactory = getConnectionFactory();
        this.connection = this.connectionFactory.create().block();
    }

    @AfterEach
    void tearDown() {
        this.connection.close().as(StepVerifier::create).verifyComplete();
    }
}
