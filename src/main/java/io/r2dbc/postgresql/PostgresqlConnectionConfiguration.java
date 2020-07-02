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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.postgresql.client.DefaultHostnameVerifier;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.extension.Extension;
import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.net.Socket;
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

    private final Duration connectTimeout;

    private final String database;

    private final List<Extension> extensions;

    private final ToIntFunction<String> fetchSize;

    private final boolean forceBinary;

    private final String host;

    private final Map<String, String> options;

    private final CharSequence password;

    private final int port;

    private final String socket;

    private final String username;

    private final SSLConfig sslConfig;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final int preparedStatementCacheQueries;

    private PostgresqlConnectionConfiguration(String applicationName, boolean autodetectExtensions, @Nullable Duration connectTimeout, @Nullable String database, List<Extension> extensions,
                                              ToIntFunction<String> fetchSize, boolean forceBinary, @Nullable String host, @Nullable Map<String, String> options, @Nullable CharSequence password,
                                              int port, @Nullable String schema, @Nullable String socket, boolean tcpKeepAlive, boolean tcpNoDelay, String username, SSLConfig sslConfig,
                                              int preparedStatementCacheQueries) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.autodetectExtensions = autodetectExtensions;
        this.connectTimeout = connectTimeout;
        this.extensions = Assert.requireNonNull(extensions, "extensions must not be null");
        this.database = database;
        this.fetchSize = fetchSize;
        this.forceBinary = forceBinary;
        this.host = host;
        this.options = options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);

        if (schema != null && !schema.isEmpty()) {
            this.options.put("search_path", schema);
        }

        this.password = password;
        this.port = port;
        this.socket = socket;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.sslConfig = sslConfig;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.preparedStatementCacheQueries = preparedStatementCacheQueries;
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
            ", connectTimeout=" + this.connectTimeout +
            ", database='" + this.database + '\'' +
            ", extensions=" + this.extensions +
            ", fetchSize=" + this.fetchSize +
            ", forceBinary='" + this.forceBinary + '\'' +
            ", host='" + this.host + '\'' +
            ", options='" + this.options + '\'' +
            ", password='" + obfuscate(this.password != null ? this.password.length() : 0) + '\'' +
            ", port=" + this.port +
            ", tcpKeepAlive=" + this.tcpKeepAlive +
            ", tcpNoDelay=" + this.tcpNoDelay +
            ", username='" + this.username + '\'' +
            '}';
    }

    String getApplicationName() {
        return this.applicationName;
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

    int getPreparedStatementCacheQueries() {
        return this.preparedStatementCacheQueries;
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

        @Nullable
        private Duration connectTimeout;

        @Nullable
        private String database;

        private List<Extension> extensions = new ArrayList<>();

        private ToIntFunction<String> fetchSize = sql -> NO_LIMIT;

        private boolean forceBinary = false;

        @Nullable
        private String host;

        private Map<String, String> options;

        @Nullable
        private CharSequence password;

        private int port = DEFAULT_PORT;

        @Nullable
        private String schema;

        @Nullable
        private String socket;

        @Nullable
        private String sslCert = null;

        private HostnameVerifier sslHostnameVerifier = DefaultHostnameVerifier.INSTANCE;

        @Nullable
        private String sslKey = null;

        private SSLMode sslMode = SSLMode.DISABLE;

        @Nullable
        private CharSequence sslPassword = null;

        @Nullable
        private String sslRootCert = null;

        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer = Function.identity();

        private boolean tcpKeepAlive;

        private boolean tcpNoDelay;

        @Nullable
        private String username;

        private int preparedStatementCacheQueries = -1;

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

            return new PostgresqlConnectionConfiguration(this.applicationName, this.autodetectExtensions, this.connectTimeout, this.database, this.extensions, this.fetchSize, this.forceBinary,
                this.host, this.options, this.password, this.port, this.schema, this.socket, this.tcpKeepAlive, this.tcpNoDelay, this.username, this.createSslConfig(),
                this.preparedStatementCacheQueries);
        }

        /**
         * Configures the connection timeout. Default unconfigured.
         *
         * @param connectTimeout the connection timeout
         * @return this {@link Builder}
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Registers a {@link CodecRegistrar} that can contribute extension {@link Codec}s.
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
         * Set the default number of rows to return when fetching results from a query. If the value specified is zero, then the hint is ignored and queries request all rows when running a statement.
         *
         * @param fetchSize the number of rows to fetch
         * @return this {@code Builder}
         * @throws IllegalArgumentException if {@code fetchSize} is negative
         * @since 0.8.2
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
         * @since 0.8.2
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
         * Configure ssl cert for client certificate authentication.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslCert(String sslCert) {
            this.sslCert = Assert.requireFileExistsOrNull(sslCert, "sslCert must not be null and must exist");
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
         * Configure ssl key for client certificate authentication.
         *
         * @param sslKey a PKCS#8 private key file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslKey(String sslKey) {
            this.sslKey = Assert.requireFileExistsOrNull(sslKey, "sslKey must not be null and must exist");
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
         * Configure ssl root cert for server certificate validation.
         *
         * @param sslRootCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslRootCert(String sslRootCert) {
            this.sslRootCert = Assert.requireFileExistsOrNull(sslRootCert, "sslRootCert must not be null and must exist");
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

        @Override
        public String toString() {
            return "Builder{" +
                "applicationName='" + this.applicationName + '\'' +
                ", autodetectExtensions='" + this.autodetectExtensions + '\'' +
                ", connectTimeout='" + this.connectTimeout + '\'' +
                ", database='" + this.database + '\'' +
                ", extensions='" + this.extensions + '\'' +
                ", fetchSize='" + this.fetchSize + '\'' +
                ", forceBinary='" + this.forceBinary + '\'' +
                ", host='" + this.host + '\'' +
                ", parameters='" + this.options + '\'' +
                ", password='" + obfuscate(this.password != null ? this.password.length() : 0) + '\'' +
                ", port=" + this.port +
                ", schema='" + this.schema + '\'' +
                ", username='" + this.username + '\'' +
                ", socket='" + this.socket + '\'' +
                ", sslContextBuilderCustomizer='" + this.sslContextBuilderCustomizer + '\'' +
                ", sslMode='" + this.sslMode + '\'' +
                ", sslRootCert='" + this.sslRootCert + '\'' +
                ", sslCert='" + this.sslCert + '\'' +
                ", sslKey='" + this.sslKey + '\'' +
                ", sslHostnameVerifier='" + this.sslHostnameVerifier + '\'' +
                ", tcpKeepAlive='" + this.tcpKeepAlive + '\'' +
                ", tcpNoDelay='" + this.tcpNoDelay + '\'' +
                ", preparedStatementCacheQueries='" + this.preparedStatementCacheQueries + '\'' +
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
                    sslContextBuilder.trustManager(new File(this.sslRootCert));
                }
            } else {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }

            String sslKey = this.sslKey;
            String sslCert = this.sslCert;

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
                if (new File(pathname).exists()) {
                    sslCert = pathname;
                }
            }

            if (sslKey == null) {
                String pathname = defaultDir + "postgresql.pk8";
                if (new File(pathname).exists()) {
                    sslKey = pathname;
                }
            }

            if (sslKey != null && sslCert != null) {
                String sslPassword = this.sslPassword == null ? null : this.sslPassword.toString();
                sslContextBuilder.keyManager(new File(sslCert), new File(sslKey), sslPassword);
            }

            return () -> SslProvider.builder()
                .sslContext(this.sslContextBuilderCustomizer.apply(sslContextBuilder))
                .defaultConfiguration(TCP)
                .build();
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
