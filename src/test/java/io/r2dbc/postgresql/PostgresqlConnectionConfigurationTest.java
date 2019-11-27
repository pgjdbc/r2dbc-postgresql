/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.postgresql.client.SSLMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlConnectionConfigurationTest {

    @Test
    void builderNoApplicationName() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().applicationName(null))
            .withMessage("applicationName must not be null");
    }

    @Test
    void builderNoHostConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().build())
            .withMessage("Either multiple hosts configuration or single host configuration should be provided");
    }

    @Test
    void builderHostAndSocket() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().host("host").socket("socket").build())
            .withMessageContaining("either host/port or socket");
    }

    @Test
    void builderNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder().username(null))
            .withMessage("username must not be null");
    }

    @Test
    void configuration() {
        Map<String, String> options = new HashMap<>();
        options.put("lock_timeout", "10s");
        options.put("statement_timeout", "60000"); // [ms]

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .connectTimeout(Duration.ofMillis(1000))
            .database("test-database")
            .host("test-host")
            .options(options)
            .port(100)
            .schema("test-schema")
            .username("test-username")
            .sslMode(SSLMode.ALLOW)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("applicationName", "test-application-name")
            .hasFieldOrPropertyWithValue("connectTimeout", Duration.ofMillis(1000))
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("singleHostConfiguration.host", "test-host")
            .hasFieldOrPropertyWithValue("singleHostConfiguration.port", 100)
            .hasFieldOrPropertyWithValue("options", options)
            .hasFieldOrPropertyWithValue("password", null)
            .hasFieldOrPropertyWithValue("schema", "test-schema")
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrProperty("sslConfig");
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
            .hasFieldOrPropertyWithValue("applicationName", "r2dbc-postgresql")
            .hasFieldOrPropertyWithValue("database", "test-database")
            .hasFieldOrPropertyWithValue("singleHostConfiguration.host", "test-host")
            .hasFieldOrPropertyWithValue("singleHostConfiguration.port", 5432)
            .hasFieldOrPropertyWithValue("password", "test-password")
            .hasFieldOrPropertyWithValue("schema", "test-schema")
            .hasFieldOrPropertyWithValue("username", "test-username")
            .hasFieldOrProperty("sslConfig");
    }

    @Test
    void constructorNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .build())
            .withMessage("username must not be null");
    }

    @Test
    void constructorInvalidOptions() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .host("test-host")
            .username("test-username")
            .password("test-password")
            .options(null)
            .build())
            .withMessage("options map must not be null");

        assertThatIllegalArgumentException().isThrownBy(() -> {
            final Map<String, String> options = new HashMap<>();
            options.put(null, "test-value");
            PostgresqlConnectionConfiguration.builder()
                .host("test-host")
                .username("test-username")
                .password("test-password")
                .options(options)
                .build();
        })
            .withMessage("option keys must not be null");

        assertThatIllegalArgumentException().isThrownBy(() -> {
            final Map<String, String> options = new HashMap<>();
            options.put("test-option", null);
            PostgresqlConnectionConfiguration.builder()
                .host("test-host")
                .username("test-username")
                .password("test-password")
                .options(options)
                .build();
        })
            .withMessage("option values must not be null");
    }

    @Test
    void constructorNoSslCustomizer() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlConnectionConfiguration.builder()
            .sslContextBuilderCustomizer(null)
            .build())
            .withMessage("sslContextBuilderCustomizer must not be null");
    }

}
