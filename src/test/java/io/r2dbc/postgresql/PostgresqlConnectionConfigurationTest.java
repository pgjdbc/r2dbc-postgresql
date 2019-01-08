/*
 * Copyright 2017-2019 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlConnectionConfigurationTest {

    @Test
    void builderNoApplicationName() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().applicationName(null))
            .withMessage("applicationName must not be null");
    }

    @Test
    void builderNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().host(null))
            .withMessage("host must not be null");
    }

    @Test
    void builderNoPassword() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().password(null))
            .withMessage("password must not be null");
    }

    @Test
    void builderNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().username(null))
            .withMessage("username must not be null");
    }

    @Test
    void configuration() {
        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .port(100)
            .schema("test-schema")
            .username("test-username")
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("applicationName", "test-application-name")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 100)
            .hasFieldOrPropertyWithValue("schema", "test-schema")
            .hasFieldOrPropertyWithValue("username", "test-username");
    }

    @Test
    void configurationDefaults() {
        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .database("test-database")
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .schema("test-schema")
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("applicationName", "postgresql-r2dbc")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("host", "test-host")
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("port", 5432)
            .hasFieldOrPropertyWithValue("schema", "test-schema")
            .hasFieldOrPropertyWithValue("username", "test-username");
    }

    @Test
    void constructorNoNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .password("test-password")
            .username("test-username")
            .build())
            .withMessage("host must not be null");
    }

    @Test
    void constructorNoPassword() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .host("test-host")
            .username("test-username")
            .build())
            .withMessage("password must not be null");
    }

    @Test
    void constructorNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .build())
            .withMessage("username must not be null");
    }

}
