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
import java.util.LinkedHashMap;
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

    /**
     * Configure the builder with the given {@link ConnectionFactoryOptions}.
     *
     * @param options {@link ConnectionFactoryOptions}
     * @return this {@link PostgresqlConnectionConfiguration.Builder}
     * @throws IllegalArgumentException if {@code options} is {@code null}
     */
    private static PostgresqlConnectionConfiguration.Builder fromConnectionFactoryOptions(ConnectionFactoryOptions options) {

        Assert.requireNonNull(options, "connectionFactoryOptions must not be null");

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder();

        OptionMapper mapper = OptionMapper.create(options);

        mapper.from(APPLICATION_NAME).to(builder::applicationName);
        mapper.from(AUTODETECT_EXTENSIONS).map(OptionMapper::toBoolean).to(builder::autodetectExtensions);
        mapper.from(CONNECT_TIMEOUT).map(OptionMapper::toDuration).to(builder::connectTimeout);
        mapper.from(CURRENT_SCHEMA).to(builder::schema).otherwise(() -> mapper.from(SCHEMA).to(builder::schema));
        mapper.from(DATABASE).to(builder::database);
        mapper.from(ERROR_RESPONSE_LOG_LEVEL).map(it -> OptionMapper.toEnum(it, LogLevel.class)).to(builder::errorResponseLogLevel);
        mapper.from(FETCH_SIZE).map(OptionMapper::toInteger).to(builder::fetchSize);
        mapper.from(FORCE_BINARY).map(OptionMapper::toBoolean).to(builder::forceBinary);
        mapper.from(NOTICE_LOG_LEVEL).map(it -> OptionMapper.toEnum(it, LogLevel.class)).to(builder::noticeLogLevel);
        mapper.from(OPTIONS).map(PostgresqlConnectionFactoryProvider::convertToMap).to(builder::options);
        mapper.from(PASSWORD).to(builder::password);
        mapper.from(PORT).map(OptionMapper::toInteger).to(builder::port);
        mapper.from(PREPARED_STATEMENT_CACHE_QUERIES).map(OptionMapper::toInteger).to(builder::preparedStatementCacheQueries);
        mapper.from(SOCKET).to(builder::socket).otherwise(() -> {
            builder.host(options.getRequiredValue(HOST));
            setupSsl(builder, mapper);
        });
        builder.username(options.getRequiredValue(USER));

        return builder;
    }

    private static void setupSsl(PostgresqlConnectionConfiguration.Builder builder, OptionMapper mapper) {

        mapper.from(SSL).to(builder::enableSsl);
        mapper.from(SSL_MODE).map(it -> {

            if (it instanceof String) {
                return SSLMode.fromValue(it.toString());
            }

            return it;

        }).to(builder::enableSsl);

        mapper.from(SSL_CERT).to(builder::sslCert);
        mapper.from(SSL_CONTEXT_BUILDER_CUSTOMIZER).to(builder::sslContextBuilderCustomizer);
        mapper.from(SSL_KEY).to(builder::sslKey);
        mapper.from(SSL_ROOT_CERT).to(builder::sslRootCert);
        mapper.from(SSL_PASSWORD).to(builder::sslPassword);

        mapper.from(SSL_HOSTNAME_VERIFIER).map(it -> {

            if (it instanceof String) {

                try {
                    Class<?> verifierClass = Class.forName((String) it);
                    Object verifier = verifierClass.getConstructor().newInstance();

                    return (HostnameVerifier) verifier;
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("Cannot instantiate " + it, e);
                }
            }

            return (HostnameVerifier) it;
        }).to(builder::sslHostnameVerifier);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> convertToMap(Object options) {
        if (options instanceof Map) {
            return Map.class.cast(options);
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : options.toString().split(";")) {
            String[] items = pair.split("=");
            if (items.length != 2) {
                throw new IllegalArgumentException(String.format("Provided options pair is not a valid name=value pair: %s", pair));
            }
            result.put(items[0], items[1]);
        }

        return result;
    }
}
