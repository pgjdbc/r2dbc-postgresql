/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;

import java.util.Collections;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

final class PostgresqlAuthenticationFailureTest {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    @Test
    void authExceptionCausedByWrongCredentials() {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(DATABASE, SERVER.getDatabase())
            .option(HOST, SERVER.getHost())
            .option(PORT, SERVER.getPort())
            .option(PASSWORD, "SUPER WRONG PASSWORD")
            .option(USER, SERVER.getUsername())
            .build());

        assertThatExceptionOfType(PostgresqlAuthenticationFailure.class)
            .isThrownBy(() -> Mono.from(connectionFactory.create()).block())
            .withMessage("password authentication failed for user \"test\"");
    }

    @Test
    void initializesReason() {
        PostgresqlAuthenticationFailure exception = new PostgresqlAuthenticationFailure(Collections.singletonList(new Field(Field.FieldType.MESSAGE, "Failed to Connect")));

        assertThat(exception.getMessage()).isEqualTo("Failed to Connect");
    }

    @Test
    void initializesSqlState() {
        PostgresqlAuthenticationFailure exception = new PostgresqlAuthenticationFailure(Collections.singletonList(new Field(Field.FieldType.CODE, "7373")));

        assertThat(exception.getSqlState()).isEqualTo("7373");
    }

    @Test
    void skipsInitializationWithEmptyFields() {
        PostgresqlAuthenticationFailure exception = new PostgresqlAuthenticationFailure(Collections.emptyList());

        assertThat(exception.getSqlState()).isNull();
        assertThat(exception.getMessage()).isNull();
    }
}
