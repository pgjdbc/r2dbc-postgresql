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

import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import javax.net.ssl.HostnameVerifier;
import java.util.Map;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

/**
 * An implementation of {@link ConnectionFactoryProvider} for creating {@link PostgresqlConnectionFactory}s.
 */
public final class PostgresqlConnectionFactoryProvider implements ConnectionFactoryProvider {

    /**
     * Application name.
     */
    public static final Option<String> APPLICATION_NAME = Option.valueOf("applicationName");

    /**
     * Auto-detect extensions.
     */
    public static final Option<Boolean> AUTODETECT_EXTENSIONS = Option.valueOf("autodetectExtensions");

    /**
     * Force binary transfer.
     */
    public static final Option<Boolean> FORCE_BINARY = Option.valueOf("forceBinary");

    /**
     * Driver option value.
     */
    public static final String POSTGRESQL_DRIVER = "postgresql";

    /**
     * Legacy driver option value.
     */
    public static final String LEGACY_POSTGRESQL_DRIVER = "postgres";

    /**
     * Schema.
     */
    public static final Option<String> SCHEMA = Option.valueOf("schema");

    /**
     * Unix domain socket.
     */
    public static final Option<String> SOCKET = Option.valueOf("socket");

    /**
     * Customizer {@link Function} for {@link SslContextBuilder}.
     */
    public static final Option<Function<SslContextBuilder, SslContextBuilder>> SSL_CONTEXT_BUILDER_CUSTOMIZER = Option.valueOf("sslContextBuilderCustomizer");

    /**
     * Full path for the certificate file.
     */
    public static final Option<String> SSL_CERT = Option.valueOf("sslCert");

    /**
     * Class name of hostname verifier. Defaults to using io.r2dbc.postgresql.client.PGHostnameVerifier
     */
    public static final Option<HostnameVerifier> SSL_HOSTNAME_VERIFIER = Option.valueOf("sslHostnameVerifier");

    /**
     * Full path for the key file.
     */
    public static final Option<String> SSL_KEY = Option.valueOf("sslKey");

    /**
     * Ssl mode. Default: disabled
     */
    public static final Option<SSLMode> SSL_MODE = Option.valueOf("sslMode");

    /**
     * SSL key password
     */
    public static final Option<String> SSL_PASSWORD = Option.valueOf("sslPassword");

    /**
     * File name of the SSL root certificate.
     */
    public static final Option<String> SSL_ROOT_CERT = Option.valueOf("sslRootCert");

    /**
     * Connection options which are applied once after the connection has been created.
     */
    public static final Option<Map<String, String>> OPTIONS = Option.valueOf("options");

    @Override
    public PostgresqlConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration
            .builder(connectionFactoryOptions)
            .build();

        return new PostgresqlConnectionFactory(configuration);
    }

    @Override
    public String getDriver() {
        return POSTGRESQL_DRIVER;
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
        return driver != null && (driver.equals(POSTGRESQL_DRIVER) || driver.equals(LEGACY_POSTGRESQL_DRIVER));
    }
}
