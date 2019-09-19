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
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.postgresql.client.DefaultHostnameVerifier;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.postgresql.codec.CodecRegistrar;
import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

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

    private final Duration connectTimeout;

    private final List<CodecRegistrar> codecRegistrars;

    private final String database;

    private final boolean forceBinary;

    private final String host;

    private final Map<String, String> options;

    private final CharSequence password;

    private final boolean registerExtensions;

    private final int port;

    private final String schema;

    private final String username;

    private final SSLConfig sslConfig;

    private PostgresqlConnectionConfiguration(String applicationName,
                                              List<CodecRegistrar> codecRegistrars, @Nullable Duration connectTimeout, @Nullable String database, boolean forceBinary, String host,
                                              @Nullable Map<String, String> options, @Nullable CharSequence password, int port, boolean registerExtensions, @Nullable String schema, String username,
                                              SSLConfig sslConfig) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.connectTimeout = connectTimeout;
        this.codecRegistrars = Assert.requireNonNull(codecRegistrars, "codecRegistrars must not be null");
        this.database = database;
        this.forceBinary = forceBinary;
        this.host = Assert.requireNonNull(host, "host must not be null");
        this.options = options;
        this.password = sslConfig.getSslMode() != SSLMode.DISABLE
            ? password
            : Assert.requireNonNull(password, "password must not be null");
        this.registerExtensions = registerExtensions;
        this.port = port;
        this.schema = schema;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.sslConfig = sslConfig;
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    private static String repeat(int length, String character) {

        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < length; i++) {
            builder.append(character);
        }

        return builder.toString();
    }

    @Override
    public String toString() {
        return "PostgresqlConnectionConfiguration{" +
            "applicationName='" + this.applicationName + '\'' +
            ", connectTimeout=" + this.connectTimeout +
            ", codecRegistrars=" + this.codecRegistrars +
            ", database='" + this.database + '\'' +
            ", forceBinary='" + this.forceBinary + '\'' +
            ", host='" + this.host + '\'' +
            ", options='" + this.options + '\'' +
            ", password='" + repeat(this.password != null ? this.password.length() : 0, "*") + '\'' +
            ", port=" + this.port +
            ", registerExtensions='" + this.registerExtensions + '\'' +
            ", schema='" + this.schema + '\'' +
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

    public List<CodecRegistrar> getCodecRegistrars() {
        return this.codecRegistrars;
    }

    String getHost() {
        return this.host;
    }

    @Nullable
    Map<String, String> getOptions() {
        return this.options;
    }

    @Nullable
    CharSequence getPassword() {
        return this.password;
    }

    int getPort() {
        return this.port;
    }

    @Nullable
    String getSchema() {
        return this.schema;
    }

    String getUsername() {
        return this.username;
    }

    boolean isForceBinary() {
        return this.forceBinary;
    }

    boolean isRegisterExtensions() {
        return this.registerExtensions;
    }

    SSLConfig getSslConfig() {
        return this.sslConfig;
    }

    /**
     * A builder for {@link PostgresqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private String applicationName = "r2dbc-postgresql";

        @Nullable
        private Duration connectTimeout;

        @Nullable
        private String database;

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
        private String sslCert = null;

        private HostnameVerifier sslHostnameVerifier = DefaultHostnameVerifier.INSTANCE;

        @Nullable
        private String sslKey = null;

        private SSLMode sslMode = SSLMode.DISABLE;

        @Nullable
        private CharSequence sslPassword = null;

        @Nullable
        private String sslRootCert = null;

        @Nullable
        private String username;

        private List<CodecRegistrar> codecRegistrars = new ArrayList<>();

        private boolean registerExtensions = true;

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
         * Returns a configured {@link PostgresqlConnectionConfiguration}.
         *
         * @return a configured {@link PostgresqlConnectionConfiguration}
         */
        public PostgresqlConnectionConfiguration build() {

            if (this.host == null) {
                throw new IllegalArgumentException("host must not be null");
            }

            if (this.username == null) {
                throw new IllegalArgumentException("username must not be null");
            }

            SSLConfig sslConfig = this.createSslConfig();
            return new PostgresqlConnectionConfiguration(this.applicationName, this.codecRegistrars, this.connectTimeout, this.database, this.forceBinary, this.host, this.options, this.password,
                this.port, this.registerExtensions, this.schema, this.username, sslConfig);
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
         *
         * @param codecRegistrar registrar to contribute codecs
         * @return this {@link Builder}
         */
        public Builder codecRegistrar(CodecRegistrar codecRegistrar) {
            this.codecRegistrars.add(Assert.requireNonNull(codecRegistrar, "codecRegistrar must not be null"));
            return this;
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
         * Configures whether to use {@link ServiceLoader} to discover and register extensions. Defaults to true.
         *
         * @param registerExtensions to discover and register extensions
         * @return this {@link Builder}
         */
        public Builder registerExtensionsFromClassPath(boolean registerExtensions) {
            this.registerExtensions = registerExtensions;
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
         * @param sslHostnameVerifier {@link javax.net.ssl.HostnameVerifier}
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

        @Override
        public String toString() {
            return "Builder{" +
                "applicationName='" + this.applicationName + '\'' +
                ", connectTimeout='" + this.connectTimeout + '\'' +
                ", database='" + this.database + '\'' +
                ", forceBinary='" + this.forceBinary + '\'' +
                ", host='" + this.host + '\'' +
                ", parameters='" + this.options + '\'' +
                ", password='" + repeat(this.password != null ? this.password.length() : 0, "*") + '\'' +
                ", registerExtensions='" + this.registerExtensions + '\'' +
                ", port=" + this.port +
                ", schema='" + this.schema + '\'' +
                ", username='" + this.username + '\'' +
                ", sslMode='" + this.sslMode + '\'' +
                ", sslRootCert='" + this.sslRootCert + '\'' +
                ", sslCert='" + this.sslCert + '\'' +
                ", sslKey='" + this.sslKey + '\'' +
                ", sslHostnameVerifier='" + this.sslHostnameVerifier + '\'' +
                '}';
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

        private SSLConfig createSslConfig() {
            if (this.sslMode == SSLMode.DISABLE) {
                return new SSLConfig(SSLMode.DISABLE, null, (hostname, session) -> true);
            }
            HostnameVerifier hostnameVerifier = this.sslHostnameVerifier;
            SslProvider sslProvider = createSslProvider();
            return new SSLConfig(this.sslMode, sslProvider, hostnameVerifier);
        }

        private SslProvider createSslProvider() {
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
            String pathsep = System.getProperty("file.separator");
            String defaultdir;
            if (System.getProperty("os.name").toLowerCase().contains("windows")) { // It is Windows
                defaultdir = System.getenv("APPDATA") + pathsep + "postgresql" + pathsep;
            } else {
                defaultdir = System.getProperty("user.home") + pathsep + ".postgresql" + pathsep;
            }

            if (sslCert == null) {
                String pathname = defaultdir + "postgresql.crt";
                if (new File(pathname).exists()) {
                    sslCert = pathname;
                }
            }

            if (sslKey == null) {
                String pathname = defaultdir + "postgresql.pk8";
                if (new File(pathname).exists()) {
                    sslKey = pathname;
                }
            }

            if (sslKey != null && sslCert != null) {
                String sslPassword = this.sslPassword == null ? null : this.sslPassword.toString();
                sslContextBuilder.keyManager(new File(sslCert), new File(sslKey), sslPassword);
            }
            return SslProvider.builder()
                .sslContext(sslContextBuilder)
                .defaultConfiguration(TCP)
                .build();
        }
    }
}
