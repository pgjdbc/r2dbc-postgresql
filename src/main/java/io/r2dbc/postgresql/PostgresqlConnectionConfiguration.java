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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.DefaultHostnameVerifier;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.extension.Extension;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.LogLevel;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static reactor.netty.tcp.SslProvider.DefaultConfigurationType.TCP;

/**
 * Connection configuration information for connecting to a PostgreSQL database.
 */
public final class PostgresqlConnectionConfiguration {

    /**
     * Default PostgreSQL port.
     */
    public static final int DEFAULT_PORT = 5432;

    private final String applicationName;

    private final boolean autodetectExtensions;

    private final boolean compatibilityMode;

    private final Duration connectTimeout;

    private final String database;

    private final LogLevel errorResponseLogLevel;

    private final List<Extension> extensions;

    private final ToIntFunction<String> fetchSize;

    private final boolean forceBinary;

    private final String host;

    @Nullable
    private final Duration lockWaitTimeout;

    @Nullable
    private final LoopResources loopResources;

    private final LogLevel noticeLogLevel;

    private final Map<String, String> options;

    private final CharSequence password;

    private final int port;

    private final boolean preferAttachedBuffers;

    private final int preparedStatementCacheQueries;

    @Nullable
    private final Duration statementTimeout;

    private final String socket;

    private final SSLConfig sslConfig;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final String username;

    private PostgresqlConnectionConfiguration(String applicationName, boolean autodetectExtensions, @Nullable boolean compatibilityMode, Duration connectTimeout, @Nullable String database,
                                              LogLevel errorResponseLogLevel,
                                              List<Extension> extensions, ToIntFunction<String> fetchSize, boolean forceBinary, @Nullable String host, @Nullable Duration lockWaitTimeout,
                                              @Nullable LoopResources loopResources,
                                              LogLevel noticeLogLevel, @Nullable Map<String, String> options, @Nullable CharSequence password, int port, boolean preferAttachedBuffers,
                                              int preparedStatementCacheQueries, @Nullable String schema, @Nullable String socket, SSLConfig sslConfig, @Nullable Duration statementTimeout,
                                              boolean tcpKeepAlive, boolean tcpNoDelay,
                                              String username) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.autodetectExtensions = autodetectExtensions;
        this.compatibilityMode = compatibilityMode;
        this.connectTimeout = connectTimeout;
        this.errorResponseLogLevel = errorResponseLogLevel;
        this.extensions = Assert.requireNonNull(extensions, "extensions must not be null");
        this.database = database;
        this.fetchSize = fetchSize;
        this.forceBinary = forceBinary;
        this.host = host;
        this.loopResources = loopResources;
        this.noticeLogLevel = noticeLogLevel;
        this.options = options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
        this.statementTimeout = statementTimeout;
        this.lockWaitTimeout = lockWaitTimeout;

        if (this.statementTimeout != null) {
            this.options.put("statement_timeout", Long.toString(statementTimeout.toMillis()));
        }

        if (this.lockWaitTimeout != null) {
            this.options.put("lock_timeout", Long.toString(lockWaitTimeout.toMillis()));
        }

        if (schema != null && !schema.isEmpty()) {
            this.options.put("search_path", schema);
        }

        this.password = password;
        this.port = port;
        this.preferAttachedBuffers = preferAttachedBuffers;
        this.preparedStatementCacheQueries = preparedStatementCacheQueries;
        this.socket = socket;
        this.sslConfig = sslConfig;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.username = Assert.requireNonNull(username, "username must not be null");
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "PostgresqlConnectionConfiguration{" +
            "applicationName='" + this.applicationName + '\'' +
            ", autodetectExtensions='" + this.autodetectExtensions + '\'' +
            ", compatibilityMode=" + this.compatibilityMode +
            ", connectTimeout=" + this.connectTimeout +
            ", errorResponseLogLevel=" + this.errorResponseLogLevel +
            ", database='" + this.database + '\'' +
            ", extensions=" + this.extensions +
            ", fetchSize=" + this.fetchSize +
            ", forceBinary='" + this.forceBinary + '\'' +
            ", host='" + this.host + '\'' +
            ", lockWaitTimeout='" + this.lockWaitTimeout +
            ", loopResources='" + this.loopResources + '\'' +
            ", noticeLogLevel='" + this.noticeLogLevel + '\'' +
            ", options='" + this.options + '\'' +
            ", password='" + obfuscate(this.password != null ? this.password.length() : 0) + '\'' +
            ", port=" + this.port +
            ", preferAttachedBuffers=" + this.preferAttachedBuffers +
            ", socket=" + this.socket +
            ", statementTimeout=" + this.statementTimeout +
            ", tcpKeepAlive=" + this.tcpKeepAlive +
            ", tcpNoDelay=" + this.tcpNoDelay +
            ", username='" + this.username + '\'' +
            '}';
    }

    String getApplicationName() {
        return this.applicationName;
    }

    boolean isCompatibilityMode() {
        return this.compatibilityMode;
    }

    @Nullable
    Duration getConnectTimeout() {
        return this.connectTimeout;
    }

    @Nullable
    String getDatabase() {
        return this.database;
    }

    List<Extension> getExtensions() {
        return this.extensions;
    }

    ToIntFunction<String> getFetchSize() {
        return this.fetchSize;
    }

    int getFetchSize(String sql) {
        return this.fetchSize.applyAsInt(sql);
    }

    @Nullable
    String getHost() {
        return this.host;
    }

    String getRequiredHost() {

        String host = getHost();

        if (host == null || host.isEmpty()) {
            throw new IllegalStateException("Connection is configured for socket connections and not for host usage");
        }

        return host;
    }

    Map<String, String> getOptions() {
        return Collections.unmodifiableMap(this.options);
    }

    @Nullable
    CharSequence getPassword() {
        return this.password;
    }

    int getPort() {
        return this.port;
    }

    boolean isPreferAttachedBuffers() {
        return this.preferAttachedBuffers;
    }

    int getPreparedStatementCacheQueries() {
        return this.preparedStatementCacheQueries;
    }

    @Nullable
    String getSocket() {
        return this.socket;
    }

    String getRequiredSocket() {

        String socket = getSocket();

        if (socket == null || socket.isEmpty()) {
            throw new IllegalStateException("Connection is configured to use host and port connections and not for socket usage");
        }

        return socket;
    }

    String getUsername() {
        return this.username;
    }

    boolean isAutodetectExtensions() {
        return this.autodetectExtensions;
    }

    boolean isForceBinary() {
        return this.forceBinary;
    }

    boolean isTcpKeepAlive() {
        return this.tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return this.tcpNoDelay;
    }

    boolean isUseSocket() {
        return getSocket() != null;
    }

    SSLConfig getSslConfig() {
        return this.sslConfig;
    }

    ConnectionSettings getConnectionSettings() {
        return ConnectionSettings.builder()
            .connectTimeout(getConnectTimeout())
            .errorResponseLogLevel(this.errorResponseLogLevel)
            .noticeLogLevel(this.noticeLogLevel)
            .sslConfig(getSslConfig())
            .tcpKeepAlive(isTcpKeepAlive())
            .tcpNoDelay(isTcpNoDelay())
            .loopResources(this.loopResources)
            .build();
    }

    private static String obfuscate(int length) {

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            builder.append("*");
        }

        return builder.toString();
    }

    /**
     * A builder for {@link PostgresqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private String applicationName = "r2dbc-postgresql";

        private boolean autodetectExtensions = true;

        private boolean compatibilityMode = false;

        @Nullable
        private Duration connectTimeout;

        @Nullable
        private String database;

        private LogLevel errorResponseLogLevel = LogLevel.DEBUG;

        private final List<Extension> extensions = new ArrayList<>();

        private ToIntFunction<String> fetchSize = sql -> NO_LIMIT;

        private boolean forceBinary = false;

        @Nullable
        private String host;

        @Nullable
        private Duration lockWaitTimeout;

        private LogLevel noticeLogLevel = LogLevel.DEBUG;

        private Map<String, String> options;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        private boolean preferAttachedBuffers = false;

        private int preparedStatementCacheQueries = -1;

        @Nullable
        private String schema;

        @Nullable
        private String socket;

        @Nullable
        private URL sslCert = null;

        private HostnameVerifier sslHostnameVerifier = DefaultHostnameVerifier.INSTANCE;

        @Nullable
        private URL sslKey = null;

        private SSLMode sslMode = SSLMode.DISABLE;

        @Nullable
        private CharSequence sslPassword = null;

        @Nullable
        private URL sslRootCert = null;

        @Nullable
        private Duration statementTimeout = null;

        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer = Function.identity();

        private boolean tcpKeepAlive = false;

        private boolean tcpNoDelay = true;

        @Nullable
        private LoopResources loopResources = null;

        @Nullable
        private String username;

        private Builder() {
        }

        /**
         * Configure the application name.  Defaults to {@code postgresql-r2dbc}.
         *
         * @param applicationName the application name
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code applicationName} is {@code null}
         */
        public Builder applicationName(String applicationName) {
            this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
            return this;
        }

        /**
         * Configures whether to use {@link ServiceLoader} to discover and register extensions. Defaults to true.
         *
         * @param autodetectExtensions to discover and register extensions
         * @return this {@link Builder}
         */
        public Builder autodetectExtensions(boolean autodetectExtensions) {
            this.autodetectExtensions = autodetectExtensions;
            return this;
        }

        /**
         * Returns a configured {@link PostgresqlConnectionConfiguration}.
         *
         * @return a configured {@link PostgresqlConnectionConfiguration}
         */
        public PostgresqlConnectionConfiguration build() {

            if (this.host == null && this.socket == null) {
                throw new IllegalArgumentException("host or socket must not be null");
            }

            if (this.host != null && this.socket != null) {
                throw new IllegalArgumentException("Connection must be configured for either host/port or socket usage but not both");
            }

            if (this.username == null) {
                throw new IllegalArgumentException("username must not be null");
            }

            return new PostgresqlConnectionConfiguration(this.applicationName, this.autodetectExtensions, this.compatibilityMode, this.connectTimeout, this.database, this.errorResponseLogLevel,
                this.extensions,
                this.fetchSize
                , this.forceBinary, this.host, this.lockWaitTimeout, this.loopResources, this.noticeLogLevel, this.options, this.password, this.port, this.preferAttachedBuffers,
                this.preparedStatementCacheQueries, this.schema, this.socket, this.createSslConfig(), this.statementTimeout, this.tcpKeepAlive, this.tcpNoDelay, this.username);
        }

        /**
         * Enables protocol compatibility mode for cursored query execution. Cursored query execution applies when configuring a non-zero fetch size. Compatibility mode uses {@code Execute+Sync}
         * messages instead of {@code Execute+Flush}. The default mode uses optimized fetching which does not work with newer pgpool versions.
         *
         * @param compatibilityMode whether to enable compatibility mode
         * @return this {@link Builder}
         * @since 0.8.7
         */
        public Builder compatibilityMode(boolean compatibilityMode) {
            this.compatibilityMode = compatibilityMode;
            return this;
        }

        /**
         * Configure the connection timeout. Default unconfigured.
         *
         * @param connectTimeout the connection timeout
         * @return this {@link Builder}
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Register a {@link CodecRegistrar} that can contribute extension {@link Codec}s.
         * Calling this method adds a {@link CodecRegistrar} and does not replace existing {@link Extension}s.
         *
         * @param codecRegistrar registrar to contribute codecs
         * @return this {@link Builder}
         */
        public Builder codecRegistrar(CodecRegistrar codecRegistrar) {
            return extendWith(codecRegistrar);
        }

        /**
         * Configure the database.
         *
         * @param database the database
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Enable SSL usage. This flag is also known as Use Encryption in other drivers.
         *
         * @return this {@link Builder}
         */
        public Builder enableSsl() {
            return sslMode(SSLMode.VERIFY_FULL);
        }

        /**
         * Registers a {@link Extension} to extend driver functionality.
         * Calling this method adds a {@link Extension} and does not replace existing {@link Extension}s.
         *
         * @param extension extension to extend driver functionality
         * @return this {@link Builder}
         */
        public Builder extendWith(Extension extension) {
            this.extensions.add(Assert.requireNonNull(extension, "extension must not be null"));
            return this;
        }

        /**
         * Configure the {@link LogLevel} for {@link ErrorResponse error responses} that are part of a statement execution.
         *
         * @param errorResponseLogLevel the log level to use.
         * @return this {@link Builder}
         * @since 0.9
         */
        public Builder errorResponseLogLevel(LogLevel errorResponseLogLevel) {
            this.errorResponseLogLevel = Assert.requireNonNull(errorResponseLogLevel, "errorResponseLogLevel must not be null");
            return this;
        }

        /**
         * Set the default number of rows to return when fetching results from a query. If the value specified is zero, then the hint is ignored and queries request all rows when running a statement.
         *
         * @param fetchSize the number of rows to fetch
         * @return this {@code Builder}
         * @throws IllegalArgumentException if {@code fetchSize} is {@code negative}
         * @since 0.9
         */
        public Builder fetchSize(int fetchSize) {
            Assert.isTrue(fetchSize >= 0, "fetch size must be greater or equal zero");
            return fetchSize(new FixedFetchSize(fetchSize));
        }

        /**
         * Set a function that maps a SQL query to the number of rows to return when fetching results for that query.
         *
         * @param fetchSizeFunction a function that maps the number of rows to fetch
         * @return this {@code Builder}
         * @throws IllegalArgumentException if {@code fetchSizeFunction} is {@code null}
         * @since 0.9
         */
        public Builder fetchSize(ToIntFunction<String> fetchSizeFunction) {
            Assert.requireNonNull(fetchSizeFunction, "fetch size function must be non null");
            this.fetchSize = fetchSizeFunction;
            return this;
        }

        /**
         * Force binary results (<a href="https://wiki.postgresql.org/wiki/JDBC-BinaryTransfer">Binary Transfer</a>). Defaults to false.
         *
         * @param forceBinary whether to force binary transfer
         * @return this {@link Builder}
         */
        public Builder forceBinary(boolean forceBinary) {
            this.forceBinary = forceBinary;
            return this;
        }

        /**
         * Configure the host.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder host(String host) {
            this.host = Assert.requireNonNull(host, "host must not be null");
            return this;
        }

        /**
         * Configure the Lock wait timeout. Default unconfigured.
         * <p>
         * This parameter is applied once after creating a new connection.
         * If lockTimeout is already set using {@link #options(Map)}, it will be overridden.
         * <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-FORMAT">Lock Timeout</a>
         *
         * @param lockWaitTimeout the lock timeout
         * @return this {@link Builder}
         * @since 0.8.9
         */
        public Builder lockWaitTimeout(Duration lockWaitTimeout) {
            this.lockWaitTimeout = Assert.requireNonNull(lockWaitTimeout, "Lock wait timeout must not be null");
            return this;
        }

        /**
         * Configure {@link LoopResources}.
         *
         * @param loopResources the {@link LoopResources}
         * @return this {@link Builder}
         * @since 0.8.5
         */
        public Builder loopResources(LoopResources loopResources) {
            this.loopResources = Assert.requireNonNull(loopResources, "loopResources must not be null");
            return this;
        }

        /**
         * Configure the {@link LogLevel} for {@link NoticeResponse notice responses}.
         *
         * @param noticeLogLevel the log level to use.
         * @return this {@link Builder}
         * @since 0.9
         */
        public Builder noticeLogLevel(LogLevel noticeLogLevel) {
            this.noticeLogLevel = Assert.requireNonNull(noticeLogLevel, "noticeLogLevel must not be null");
            return this;
        }

        /**
         * Configure connection initialization parameters.
         * <p>
         * These parameters are applied once after creating a new connection. This is useful for setting up client-specific
         * <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-FORMAT">runtime parameters</a>
         * like statement timeouts, time zones etc.
         *
         * @param options the options
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code options} or any key or value of {@code options} is {@code null}
         */
        public Builder options(Map<String, String> options) {
            Assert.requireNonNull(options, "options map must not be null");

            options.forEach((k, v) -> {
                Assert.requireNonNull(k, "option keys must not be null");
                Assert.requireNonNull(v, "option values must not be null");
            });

            this.options = options;
            return this;
        }

        /**
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = password;
            return this;
        }

        /**
         * Configure the port.  Defaults to {@code 5432}.
         *
         * @param port the port
         * @return this {@link Builder}
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Configure whether {@link Codecs codecs} should prefer attached data buffers. The default is {@code false}, meaning that codecs will copy data from the input buffer into a {@code byte[]}
         * or similar data structure that is enabled for garbage collection.  Using attached buffers is more efficient but comes with the requirement that decoded values (such as {@link Json}) must
         * be consumed to release attached buffers to avoid memory leaks.
         *
         * @param preferAttachedBuffers the flag whether to prefer attached buffers
         * @return this {@link Builder}
         * @since 0.8.5
         */
        public Builder preferAttachedBuffers(boolean preferAttachedBuffers) {
            this.preferAttachedBuffers = preferAttachedBuffers;
            return this;
        }

        /**
         * Configure the preparedStatementCacheQueries. The default is {@code -1}, meaning there's no limit. The value of {@code 0} disables the cache. Any other value specifies the cache size.
         *
         * @param preparedStatementCacheQueries the preparedStatementCacheQueries
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code username} is {@code null}
         * @since 0.8.1
         */
        public Builder preparedStatementCacheQueries(int preparedStatementCacheQueries) {
            this.preparedStatementCacheQueries = preparedStatementCacheQueries;
            return this;
        }

        /**
         * Configure the schema.
         *
         * @param schema the schema
         * @return this {@link Builder}
         */
        public Builder schema(@Nullable String schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Configure the unix domain socket to connect to.
         *
         * @param socket the socket path
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code socket} is {@code null}
         */
        public Builder socket(String socket) {
            this.socket = Assert.requireNonNull(socket, "host must not be null");
            sslMode(SSLMode.DISABLE);
            return this;
        }

        /**
         * Configure a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL connection attempt to allow for just-in-time configuration updates. The {@link Function} gets
         * called with the prepared {@link SslContextBuilder} that has all configuration options applied. The customizer may return the same builder or return a new builder instance to be used to
         * build the SSL context.
         *
         * @param sslContextBuilderCustomizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslContextBuilderCustomizer} is {@code null}
         */
        public Builder sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
            this.sslContextBuilderCustomizer = Assert.requireNonNull(sslContextBuilderCustomizer, "sslContextBuilderCustomizer must not be null");
            return this;
        }

        /**
         * Configure ssl cert for client certificate authentication. Can point to either a resource within the classpath or a file.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslCert(String sslCert) {
            return sslCert(requireExistingFilePath(sslCert, "sslCert must not be null and must exist"));
        }

        /**
         * Configure ssl cert for client certificate authentication.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         * @since 0.8.7
         */
        public Builder sslCert(URL sslCert) {
            this.sslCert = Assert.requireNonNull(sslCert, "sslCert must not be null");
            return this;
        }

        /**
         * Configure ssl HostnameVerifier.
         *
         * @param sslHostnameVerifier {@link HostnameVerifier}
         * @return this {@link Builder}
         */
        public Builder sslHostnameVerifier(HostnameVerifier sslHostnameVerifier) {
            this.sslHostnameVerifier = Assert.requireNonNull(sslHostnameVerifier, "sslHostnameVerifier must not be null");
            return this;
        }

        /**
         * Configure ssl key for client certificate authentication.  Can point to either a resource within the classpath or a file.
         *
         * @param sslKey a PKCS#8 private key file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslKey(String sslKey) {
            return sslKey(requireExistingFilePath(sslKey, "sslKey must not be null and must exist"));
        }

        /**
         * Configure ssl key for client certificate authentication.
         *
         * @param sslKey a PKCS#8 private key file in PEM format
         * @return this {@link Builder}
         * @since 0.8.7
         */
        public Builder sslKey(URL sslKey) {
            this.sslKey = Assert.requireNonNull(sslKey, "sslKey must not be null");
            return this;
        }

        /**
         * Configure ssl mode.
         *
         * @param sslMode the SSL mode to use.
         * @return this {@link Builder}
         */
        public Builder sslMode(SSLMode sslMode) {
            this.sslMode = Assert.requireNonNull(sslMode, "sslMode must be not be null");
            return this;
        }

        /**
         * Configure ssl password.
         *
         * @param sslPassword the password of the sslKey, or null if it's not password-protected
         * @return this {@link Builder}
         */
        public Builder sslPassword(@Nullable CharSequence sslPassword) {
            this.sslPassword = sslPassword;
            return this;
        }

        /**
         * Configure ssl root cert for server certificate validation. Can point to either a resource within the classpath or a file.
         *
         * @param sslRootCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslRootCert(String sslRootCert) {
            return sslRootCert(requireExistingFilePath(sslRootCert, "sslRootCert must not be null and must exist"));
        }

        /**
         * Configure ssl root cert for server certificate validation.
         *
         * @param sslRootCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         * @since 0.8.7
         */
        public Builder sslRootCert(URL sslRootCert) {
            this.sslRootCert = Assert.requireNonNull(sslRootCert, "sslRootCert must not be null and must exist");
            return this;
        }

        /**
         * Configure the Statement timeout. Default unconfigured.
         * <p>
         * This parameter is applied once after creating a new connection.
         * If statementTimeout is already set using {@link #options(Map)}, it will be overridden.
         * <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#RUNTIME-CONFIG-CLIENT-FORMAT">Statement Timeout</a>
         *
         * @param statementTimeout the statement timeout
         * @return this {@link Builder}
         * @since 0.8.9
         */
        public Builder statementTimeout(Duration statementTimeout) {
            this.statementTimeout = Assert.requireNonNull(statementTimeout, "Statement timeout");
            return this;
        }

        /**
         * Configure TCP KeepAlive.
         *
         * @param enabled whether to enable TCP KeepAlive
         * @return this {@link Builder}
         * @see Socket#setKeepAlive(boolean)
         * @since 0.8.4
         */
        public Builder tcpKeepAlive(boolean enabled) {
            this.tcpKeepAlive = enabled;
            return this;
        }

        /**
         * Configure TCP NoDelay.
         *
         * @param enabled whether to enable TCP NoDelay
         * @return this {@link Builder}
         * @see Socket#setTcpNoDelay(boolean)
         * @since 0.8.4
         */
        public Builder tcpNoDelay(boolean enabled) {
            this.tcpNoDelay = enabled;
            return this;
        }

        /**
         * Configure the username.
         *
         * @param username the username
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code username} is {@code null}
         */
        public Builder username(String username) {
            this.username = Assert.requireNonNull(username, "username must not be null");
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "applicationName='" + this.applicationName + '\'' +
                ", autodetectExtensions='" + this.autodetectExtensions + '\'' +
                ", compatibilityMode='" + this.compatibilityMode + '\'' +
                ", connectTimeout='" + this.connectTimeout + '\'' +
                ", database='" + this.database + '\'' +
                ", extensions='" + this.extensions + '\'' +
                ", errorResponseLogLevel='" + this.errorResponseLogLevel + '\'' +
                ", fetchSize='" + this.fetchSize + '\'' +
                ", forceBinary='" + this.forceBinary + '\'' +
                ", host='" + this.host + '\'' +
                ", lockWaitTimeout='" + this.lockWaitTimeout + '\'' +
                ", loopResources='" + this.loopResources + '\'' +
                ", noticeLogLevel='" + this.noticeLogLevel + '\'' +
                ", parameters='" + this.options + '\'' +
                ", password='" + obfuscate(this.password != null ? this.password.length() : 0) + '\'' +
                ", port=" + this.port +
                ", preparedStatementCacheQueries='" + this.preparedStatementCacheQueries + '\'' +
                ", schema='" + this.schema + '\'' +
                ", socket='" + this.socket + '\'' +
                ", sslContextBuilderCustomizer='" + this.sslContextBuilderCustomizer + '\'' +
                ", sslMode='" + this.sslMode + '\'' +
                ", sslRootCert='" + this.sslRootCert + '\'' +
                ", sslCert='" + this.sslCert + '\'' +
                ", sslKey='" + this.sslKey + '\'' +
                ", statementTimeout='" + this.statementTimeout + '\'' +
                ", sslHostnameVerifier='" + this.sslHostnameVerifier + '\'' +
                ", tcpKeepAlive='" + this.tcpKeepAlive + '\'' +
                ", tcpNoDelay='" + this.tcpNoDelay + '\'' +
                ", username='" + this.username + '\'' +
                '}';
        }

        private SSLConfig createSslConfig() {
            if (this.socket != null || this.sslMode == SSLMode.DISABLE) {
                return SSLConfig.disabled();
            }

            HostnameVerifier hostnameVerifier = this.sslHostnameVerifier;
            return new SSLConfig(this.sslMode, createSslProvider(), hostnameVerifier);
        }

        private Supplier<SslProvider> createSslProvider() {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            if (this.sslMode.verifyCertificate()) {
                if (this.sslRootCert != null) {
                    doWithStream(this.sslRootCert, sslContextBuilder::trustManager);
                }
            } else {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }

            URL sslKey = this.sslKey;
            URL sslCert = this.sslCert;

            // Emulate Libpq behavior
            // Determining the default file location
            String pathSeparator = System.getProperty("file.separator");
            String defaultDir;
            if (System.getProperty("os.name").toLowerCase().contains("windows")) { // It is Windows
                defaultDir = System.getenv("APPDATA") + pathSeparator + "postgresql" + pathSeparator;
            } else {
                defaultDir = System.getProperty("user.home") + pathSeparator + ".postgresql" + pathSeparator;
            }

            if (sslCert == null) {
                String pathname = defaultDir + "postgresql.crt";
                sslCert = resolveUrlFromFile(pathname);
            }

            if (sslKey == null) {
                String pathname = defaultDir + "postgresql.pk8";
                sslKey = resolveUrlFromFile(pathname);
            }

            URL sslKeyToUse = sslKey;

            if (sslKey != null && sslCert != null) {
                String sslPassword = this.sslPassword == null ? null : this.sslPassword.toString();

                doWithStream(sslCert, certStream -> {
                    doWithStream(sslKeyToUse, keyStream -> {
                        sslContextBuilder.keyManager(certStream, keyStream, sslPassword);
                    });
                });
            }

            return () -> SslProvider.builder()
                .sslContext(this.sslContextBuilderCustomizer.apply(sslContextBuilder))
                .defaultConfiguration(TCP)
                .build();
        }

        interface StreamConsumer {

            void doWithStream(InputStream is) throws IOException;

        }

        private void doWithStream(URL url, StreamConsumer consumer) {

            try (InputStream is = url.openStream()) {

                consumer.doWithStream(is);
            } catch (IOException e) {
                throw new IllegalStateException("Error while reading " + url, e);
            }
        }

        private URL requireExistingFilePath(String path, String message) {

            Assert.requireNonNull(path, message);

            URL resource = getClass().getClassLoader().getResource(path);

            if (resource != null) {
                return resource;
            }

            if (!new File(path).exists()) {
                throw new IllegalArgumentException(message);
            }

            return resolveUrlFromFile(path);
        }

        private URL resolveUrlFromFile(String pathname) {

            File file = new File(pathname);

            if (file.exists()) {
                try {
                    return file.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(String.format("Malformed error occurred during creating URL from %s", pathname));
                }
            }

            return null;
        }

    }

    static class FixedFetchSize implements ToIntFunction<String> {

        private final int fetchSize;

        public FixedFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
        }

        @Override
        public int applyAsInt(String value) {
            return this.fetchSize;
        }

        @Override
        public String toString() {
            return "" + this.fetchSize;
        }

    }

}
