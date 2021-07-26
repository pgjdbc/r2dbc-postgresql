/*
 * Copyright 2017 the original author or authors.
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
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.postgresql.extension.Extension;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.LogLevel;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import reactor.netty.resources.LoopResources;

import javax.net.ssl.HostnameVerifier;
import java.time.Duration;
import java.util.Collection;
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
     * Compatibility query mode for cursored query execution.
     *
     * @since 0.8.7
     */
    public static final Option<Boolean> COMPATIBILITY_MODE = Option.valueOf("compatibilityMode");

    /**
     * Error Response Log Level.
     */
    public static final Option<LogLevel> ERROR_RESPONSE_LOG_LEVEL = Option.valueOf("errorResponseLogLevel");

    /**
     * Extensions to register.
     *
     * @since 0.8.9
     */
    public static final Option<Collection<Extension>> EXTENSIONS = Option.valueOf("extensions");

    /**
     * Fetch Size.
     */
    public static final Option<Integer> FETCH_SIZE = Option.valueOf("fetchSize");

    /**
     * Force binary transfer.
     */
    public static final Option<Boolean> FORCE_BINARY = Option.valueOf("forceBinary");

    /**
     * Lock timeout.
     *
     * @since 0.8.9
     */
    public static final Option<Duration> LOCK_WAIT_TIMEOUT = ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT;

    /**
     * Event {@link LoopResources}.
     *
     * @since 0.8.5
     */
    public static final Option<LoopResources> LOOP_RESOURCES = Option.valueOf("loopResources");

    /**
     * Notice Response Log Level.
     */
    public static final Option<LogLevel> NOTICE_LOG_LEVEL = Option.valueOf("noticeLogLevel");

    /**
     * Connection options which are applied once after the connection has been created.
     */
    public static final Option<Map<String, String>> OPTIONS = Option.valueOf("options");

    /**
     * Driver option value.
     */
    public static final String POSTGRESQL_DRIVER = "postgresql";

    /**
     * Legacy driver option value.
     */
    public static final String LEGACY_POSTGRESQL_DRIVER = "postgres";

    /**
     * Configure whether {@link Codecs codecs} should prefer attached data buffers. The default is {@code false}, meaning that codecs will copy data from the input buffer into a {@code byte[]}
     * or similar data structure that is enabled for garbage collection.  Using attached buffers is more efficient but comes with the requirement that decoded values (such as {@link Json}) must
     * be consumed to release attached buffers to avoid memory leaks.
     *
     * @since 0.8.5
     */
    public static final Option<Boolean> PREFER_ATTACHED_BUFFERS = Option.valueOf("preferAttachedBuffers");

    /**
     * Determine the number of queries that are cached in each connection.
     * The default is {@code -1}, meaning there's no limit. The value of {@code 0} disables the cache. Any other value specifies the cache size.
     */
    public static final Option<Integer> PREPARED_STATEMENT_CACHE_QUERIES = Option.valueOf("preparedStatementCacheQueries");

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
     * Path for the certificate file. Can point to either a resource within the classpath or a file.
     */
    public static final Option<String> SSL_CERT = Option.valueOf("sslCert");

    /**
     * Class name of hostname verifier. Defaults to {@link DefaultHostnameVerifier}.
     */
    public static final Option<HostnameVerifier> SSL_HOSTNAME_VERIFIER = Option.valueOf("sslHostnameVerifier");

    /**
     * File path for the key file. Can point to either a resource within the classpath or a file.
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
     * File path of the SSL root certificate. Can point to either a resource within the classpath or a file.
     */
    public static final Option<String> SSL_ROOT_CERT = Option.valueOf("sslRootCert");

    /**
     * Statement timeout.
     *
     * @since 0.8.9
     */
    public static final Option<Duration> STATEMENT_TIMEOUT = ConnectionFactoryOptions.STATEMENT_TIMEOUT;

    /**
     * Enable TCP KeepAlive.
     *
     * @since 0.8.4
     */
    public static final Option<Boolean> TCP_KEEPALIVE = Option.valueOf("tcpKeepAlive");

    /**
     * Enable TCP NoDelay.
     *
     * @since 0.8.4
     */
    public static final Option<Boolean> TCP_NODELAY = Option.valueOf("tcpNoDelay");

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

        String driver = "" + connectionFactoryOptions.getValue(DRIVER);
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

        mapper.fromTyped(APPLICATION_NAME).to(builder::applicationName);
        mapper.from(AUTODETECT_EXTENSIONS).map(OptionMapper::toBoolean).to(builder::autodetectExtensions);
        mapper.from(COMPATIBILITY_MODE).map(OptionMapper::toBoolean).to(builder::compatibilityMode);
        mapper.from(CONNECT_TIMEOUT).map(OptionMapper::toDuration).to(builder::connectTimeout);
        mapper.fromTyped(CURRENT_SCHEMA).to(builder::schema).otherwise(() -> mapper.fromTyped(SCHEMA).to(builder::schema));
        mapper.fromTyped(DATABASE).to(builder::database);
        mapper.from(ERROR_RESPONSE_LOG_LEVEL).map(it -> OptionMapper.toEnum(it, LogLevel.class)).to(builder::errorResponseLogLevel);
        mapper.fromTyped(EXTENSIONS).to(extensions -> extensions.forEach(builder::extendWith));
        mapper.from(FETCH_SIZE).map(OptionMapper::toInteger).to(builder::fetchSize);
        mapper.from(FORCE_BINARY).map(OptionMapper::toBoolean).to(builder::forceBinary);
        mapper.from(LOCK_WAIT_TIMEOUT).map(OptionMapper::toDuration).to(builder::lockWaitTimeout);
        mapper.fromTyped(LOOP_RESOURCES).to(builder::loopResources);
        mapper.from(NOTICE_LOG_LEVEL).map(it -> OptionMapper.toEnum(it, LogLevel.class)).to(builder::noticeLogLevel);
        mapper.from(OPTIONS).map(PostgresqlConnectionFactoryProvider::convertToMap).to(builder::options);
        mapper.fromTyped(PASSWORD).to(builder::password);
        mapper.from(PORT).map(OptionMapper::toInteger).to(builder::port);
        mapper.from(PREFER_ATTACHED_BUFFERS).map(OptionMapper::toBoolean).to(builder::preferAttachedBuffers);
        mapper.from(PREPARED_STATEMENT_CACHE_QUERIES).map(OptionMapper::toInteger).to(builder::preparedStatementCacheQueries);
        mapper.fromTyped(SOCKET).to(builder::socket).otherwise(() -> {
            builder.host("" + options.getRequiredValue(HOST));
            setupSsl(builder, mapper);
        });
        mapper.from(STATEMENT_TIMEOUT).map(OptionMapper::toDuration).to(builder::statementTimeout);
        mapper.from(TCP_KEEPALIVE).map(OptionMapper::toBoolean).to(builder::tcpKeepAlive);
        mapper.from(TCP_NODELAY).map(OptionMapper::toBoolean).to(builder::tcpNoDelay);
        builder.username("" + options.getRequiredValue(USER));

        return builder;
    }

    private static void setupSsl(PostgresqlConnectionConfiguration.Builder builder, OptionMapper mapper) {

        mapper.from(SSL).to(builder::enableSsl);
        mapper.from(SSL_MODE).map(it -> {

            if (it instanceof String) {
                return SSLMode.fromValue(it.toString());
            }

            return (SSLMode) it;

        }).to(builder::sslMode);

        mapper.fromTyped(SSL_CERT).to(builder::sslCert);
        mapper.fromTyped(SSL_CONTEXT_BUILDER_CUSTOMIZER).to(builder::sslContextBuilderCustomizer);
        mapper.fromTyped(SSL_KEY).to(builder::sslKey);
        mapper.fromTyped(SSL_ROOT_CERT).to(builder::sslRootCert);
        mapper.fromTyped(SSL_PASSWORD).to(builder::sslPassword);

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
