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

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

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
     * Legacy driver option value.
     *
     * @deprecated Should use {@link #POSTGRESQL_DRIVER} for new driver development.
     */
    @Deprecated
    public static final String LEGACY_POSTGRESQL_DRIVER = "postgres";

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

        builder.connectTimeout(connectionFactoryOptions.getValue(CONNECT_TIMEOUT));
        builder.database(connectionFactoryOptions.getValue(DATABASE));
        builder.host(connectionFactoryOptions.getRequiredValue(HOST));
        builder.password(connectionFactoryOptions.getRequiredValue(PASSWORD).toString());
        builder.schema(connectionFactoryOptions.getValue(SCHEMA));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));

        Integer port = connectionFactoryOptions.getValue(PORT);
        if (port != null) {
            builder.port(port);
        }

        return new PostgresqlConnectionFactory(builder.build());
    }

    @Override
    public String getDriver() {
        return POSTGRESQL_DRIVER;
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
        if (driver == null || !(driver.equals(POSTGRESQL_DRIVER) || driver.equals(LEGACY_POSTGRESQL_DRIVER))) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(HOST)) {
            return false;
        }

        if (!connectionFactoryOptions.hasOption(PASSWORD)) {
            return false;
        }

        return connectionFactoryOptions.hasOption(USER);

    }

}
