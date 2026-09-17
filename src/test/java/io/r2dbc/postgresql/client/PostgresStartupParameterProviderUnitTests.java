/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.frontend.StartupMessage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgresStartupParameterProvider}.
 */
final class PostgresStartupParameterProviderUnitTests {

    @Test
    void acceptIgnoresUserClientEncodingOverrideCaseInsensitive() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("client_encoding", "LATIN1");
        options.put("Client_Encoding", "LATIN1");
        options.put("lock_timeout", "5s");

        PostgresStartupParameterProvider provider =
            new PostgresStartupParameterProvider("test-app", TimeZone.getTimeZone("UTC"), options);

        Map<String, String> parameters = new LinkedHashMap<>();
        StartupMessage.ParameterWriter writer = parameters::put;
        provider.accept(writer);

        assertThat(parameters)
            .containsEntry("client_encoding", "utf8")
            .containsEntry("lock_timeout", "5s")
            .doesNotContainKey("Client_Encoding")
            .doesNotContainValue("LATIN1");
    }

}
