/*
 * Copyright 2017-2020 the original author or authors.
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
import io.r2dbc.postgresql.client.DefaultHostnameVerifier;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.LogLevel;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import javax.net.ssl.HostnameVerifier;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
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
     * Auto-detect extensions.
     */
    public static final Option<Boolean> AUTODETECT_EXTENSIONS = Option.valueOf("autodetectExtensions");

    /**
     * Error Response Log Level.
     */
    public static final Option<LogLevel> ERROR_RESPONSE_LOG_LEVEL = Option.valueOf("errorResponseLogLevel");

    /**
     * Fetch Size.
     */
    public static final Option<Integer> FETCH_SIZE = Option.valueOf("fetchSize");

    /**
     * Force binary transfer.
     */
    public static final Option<Boolean> FORCE_BINARY = Option.valueOf("forceBinary");

    /**
     * Notice Response Log Level.
     */
    public static final Option<LogLevel> NOTICE_LOG_LEVEL = Option.valueOf("noticeLogLevel");

    /**
     * Driver option value.
     */
    public static final String POSTGRESQL_DRIVER = "postgresql";

    /**
     * Legacy driver option value.
     */
    public static final String LEGACY_POSTGRESQL_DRIVER = "postgres";

    /**
     * Schema search path (alias for "currentSchema").
     */
    public static final Option<String> SCHEMA = Option.valueOf("schema");

    /**
     * Schema search path.
     *
     * @since 0.9
     */
    public static final Option<String> CURRENT_SCHEMA = Option.valueOf("currentSchema");

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
     * Class name of hostname verifier. Defaults to {@link DefaultHostnameVerifier}.
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
     * Determine the number of queries that are cached in each connection.
     * The default is {@code -1}, meaning there's no limit. The value of {@code 0} disables the cache. Any other value specifies the cache size.
     */
    public static final Option<Integer> PREPARED_STATEMENT_CACHE_QUERIES = Option.valueOf("preparedStatementCacheQueries");

    /**
     * Connection options which are applied once after the connection has been created.
     */
    public static final Option<Map<String, String>> OPTIONS = Option.valueOf("options");

    /**
     * Returns a new {@link PostgresqlConnectionConfiguration.Builder} configured with the given {@link ConnectionFactoryOptions}.
     *
     * @param connectionFactoryOptions {@link ConnectionFactoryOptions} used to initialize the {@link PostgresqlConnectionConfiguration.Builder}.
     * @return a {@link PostgresqlConnectionConfiguration.Builder}
     * @since 0.9
     */
    public static PostgresqlConnectionConfiguration.Builder builder(ConnectionFactoryOptions connectionFactoryOptions) {
        return fromConnectionFactoryOptions(connectionFactoryOptions);
    }

    @Override
    public PostgresqlConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        return new PostgresqlConnectionFactory(builder(connectionFactoryOptions).build());
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

    private static void setupSsl(PostgresqlConnectionConfiguration.Builder builder, ConnectionFactoryOptions connectionFactoryOptions) {
        Boolean ssl = connectionFactoryOptions.getValue(SSL);
        if (ssl != null && ssl) {
            builder.enableSsl();
        }

        Object sslMode = connectionFactoryOptions.getValue(SSL_MODE);
        if (sslMode != null) {
            if (sslMode instanceof String) {
                builder.sslMode(SSLMode.fromValue(sslMode.toString()));
            } else {
                builder.sslMode((SSLMode) sslMode);
            }
        }

        String sslRootCert = connectionFactoryOptions.getValue(SSL_ROOT_CERT);
        if (sslRootCert != null) {
            builder.sslRootCert(sslRootCert);
        }

        String sslCert = connectionFactoryOptions.getValue(SSL_CERT);
        if (sslCert != null) {
            builder.sslCert(sslCert);
        }

        String sslKey = connectionFactoryOptions.getValue(SSL_KEY);
        if (sslKey != null) {
            builder.sslKey(sslKey);
        }

        String sslPassword = connectionFactoryOptions.getValue(SSL_PASSWORD);
        if (sslPassword != null) {
            builder.sslPassword(sslPassword);
        }

        if (connectionFactoryOptions.hasOption(SSL_CONTEXT_BUILDER_CUSTOMIZER)) {
            builder.sslContextBuilderCustomizer(connectionFactoryOptions.getRequiredValue(SSL_CONTEXT_BUILDER_CUSTOMIZER));
        }

        setSslHostnameVerifier(builder, connectionFactoryOptions);
    }

    private static void setSslHostnameVerifier(PostgresqlConnectionConfiguration.Builder builder, ConnectionFactoryOptions connectionFactoryOptions) {
        Object sslHostnameVerifier = connectionFactoryOptions.getValue(SSL_HOSTNAME_VERIFIER);
        if (sslHostnameVerifier != null) {

            if (sslHostnameVerifier instanceof String) {

                try {
                    Class<?> verifierClass = Class.forName((String) sslHostnameVerifier);
                    Object verifier = verifierClass.getConstructor().newInstance();

                    builder.sslHostnameVerifier((HostnameVerifier) verifier);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("Cannot instantiate " + sslHostnameVerifier, e);
                }
            } else {
                builder.sslHostnameVerifier((HostnameVerifier) sslHostnameVerifier);
            }
        }
    }

    private static boolean isUsingTcp(ConnectionFactoryOptions connectionFactoryOptions) {
        return !connectionFactoryOptions.hasOption(SOCKET);
    }

    private static boolean convertToBoolean(Object value) {
        return value instanceof Boolean ? (boolean) value : Boolean.parseBoolean(value.toString());
    }

    private static <T extends Enum<T>> T convertToEnum(Object value, Class<T> enumType) {
        return enumType.isInstance(value) ? enumType.cast(value) : Enum.valueOf(enumType, value.toString().toUpperCase(Locale.US));
    }

    private static int convertToInt(Object value) {
        return value instanceof Integer ? (int) value : Integer.parseInt(value.toString());
    }

    /**
     * Configure the builder with the given {@link ConnectionFactoryOptions}.
     *
     * @param connectionFactoryOptions {@link ConnectionFactoryOptions}
     * @return this {@link PostgresqlConnectionConfiguration.Builder}
     * @throws IllegalArgumentException if {@code connectionFactoryOptions} is {@code null}
     */
    private static PostgresqlConnectionConfiguration.Builder fromConnectionFactoryOptions(ConnectionFactoryOptions connectionFactoryOptions) {

        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder();

        builder.connectTimeout(connectionFactoryOptions.getValue(CONNECT_TIMEOUT));
        builder.database(connectionFactoryOptions.getValue(DATABASE));
        builder.password(connectionFactoryOptions.getValue(PASSWORD));

        if (connectionFactoryOptions.getValue(CURRENT_SCHEMA) != null) {
            builder.schema(connectionFactoryOptions.getValue(CURRENT_SCHEMA));
        } else {
            builder.schema(connectionFactoryOptions.getValue(SCHEMA));
        }

        builder.username(connectionFactoryOptions.getRequiredValue(USER));

        String applicationName = connectionFactoryOptions.getValue(APPLICATION_NAME);
        if (applicationName != null) {
            builder.applicationName(applicationName);
        }

        Object autodetectExtensions = connectionFactoryOptions.getValue(AUTODETECT_EXTENSIONS);
        if (autodetectExtensions != null) {
            builder.autodetectExtensions(convertToBoolean(autodetectExtensions));
        }

        Integer port = connectionFactoryOptions.getValue(PORT);
        if (port != null) {
            builder.port(port);
        }

        Object errorResponseLogLevel = connectionFactoryOptions.getValue(ERROR_RESPONSE_LOG_LEVEL);
        if (errorResponseLogLevel != null) {
            builder.errorResponseLogLevel(convertToEnum(errorResponseLogLevel, LogLevel.class));
        }

        Object fetchSize = connectionFactoryOptions.getValue(FETCH_SIZE);
        if (fetchSize != null) {
            builder.fetchSize(convertToInt(fetchSize));
        }

        Object forceBinary = connectionFactoryOptions.getValue(FORCE_BINARY);
        if (forceBinary != null) {
            builder.forceBinary(convertToBoolean(forceBinary));
        }

        Object noticeLogLevel = connectionFactoryOptions.getValue(NOTICE_LOG_LEVEL);
        if (noticeLogLevel != null) {
            builder.noticeLogLevel(convertToEnum(noticeLogLevel, LogLevel.class));
        }

        Object preparedStatementCacheQueries = connectionFactoryOptions.getValue(PREPARED_STATEMENT_CACHE_QUERIES);
        if (preparedStatementCacheQueries != null) {
            builder.preparedStatementCacheQueries(convertToInt(preparedStatementCacheQueries));
        }

        Map<String, String> options = connectionFactoryOptions.getValue(OPTIONS);
        if (options != null) {
            builder.options(options);
        }

        if (isUsingTcp(connectionFactoryOptions)) {
            builder.host(connectionFactoryOptions.getRequiredValue(HOST));
            setupSsl(builder, connectionFactoryOptions);
        } else {
            builder.socket(connectionFactoryOptions.getRequiredValue(SOCKET));
        }

        return builder;
    }
}
