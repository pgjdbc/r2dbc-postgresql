/*
 * Copyright 2022 the original author or authors.
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
import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.TimeZone;

/**
 * {@link StartupMessage.StartupParameterProvider} for generic Postgres options.
 *
 * @since 1.0
 */
public final class PostgresStartupParameterProvider implements StartupMessage.StartupParameterProvider {

    private final String applicationName;

    private final TimeZone timeZone;

    private final @Nullable Map<String, String> options;

    public PostgresStartupParameterProvider(String applicationName, TimeZone timeZone, @Nullable Map<String, String> options) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.timeZone = Assert.requireNonNull(timeZone, "timeZone must not be null");
        this.options = options;
    }

    public PostgresStartupParameterProvider(String applicationName, TimeZone timeZone, ConnectionSettings settings) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.timeZone = Assert.requireNonNull(timeZone, "timeZone must not be null");
        this.options = settings.getStartupOptions();
    }

    @Override
    public void accept(StartupMessage.ParameterWriter writer) {

        writer.write("application_name", this.applicationName);
        writer.write("client_encoding", "utf8");
        writer.write("DateStyle", "ISO");
        writer.write("extra_float_digits", "2");
        writer.write("TimeZone", TimeZoneUtils.createPostgresTimeZone(this.timeZone));

        if (this.options != null) {
            for (Map.Entry<String, String> option : this.options.entrySet()) {
                writer.write(option.getKey(), option.getValue());
            }
        }

    }

}
