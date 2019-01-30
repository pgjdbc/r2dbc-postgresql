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

import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link PostgresqlConnectionFactory}s.
 */
public final class PostgresqlConnectionFactoryProvider implements ConnectionFactoryProvider {

    /**
     * Application name.
     */
    public static final Option<String> APPLICATION_NAME = Option.valueOf("applicationName");

    /**
     * Driver option value.
     */
    public static final String POSTGRESQL_DRIVER = "postgresql";

    /**
     * Schema.
     */
    public static final Option<String> SCHEMA = Option.valueOf("schema");

    @Override
    public PostgresqlConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder();

        String applicationName = connectionFactoryOptions.getValue(APPLICATION_NAME);
        if (applicationName != null) {
            builder.applicationName(applicationName);
        }

        builder.database(connectionFactoryOptions.getValue(DATABASE));
        builder.host(connectionFactoryOptions.getRequiredValue(HOST));
        builder.password(connectionFactoryOptions.getRequiredValue(PASSWORD).toString());
        builder.port(connectionFactoryOptions.getRequiredValue(PORT));
        builder.schema(connectionFactoryOptions.getValue(SCHEMA));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));
        builder.connectTimeout(connectionFactoryOptions.getValue(CONNECT_TIMEOUT));

        return new PostgresqlConnectionFactory(builder.build());
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(ConnectionFactoryOptions.DRIVER);
        if (driver == null || !driver.equals(POSTGRESQL_DRIVER)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(HOST)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(PASSWORD)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(PORT)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(USER)) {
            return false;
        }

        return true;
    }
}
