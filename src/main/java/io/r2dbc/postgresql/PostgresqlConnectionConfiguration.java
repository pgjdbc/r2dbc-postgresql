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
import io.r2dbc.postgresql.client.PGHostnameVerifier;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.time.Duration;
import java.util.Map;

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

    private final String database;

    private final boolean forceBinary;

    private final String host;

    private final Map<String, String> options;

    private final String password;

    private final int port;

    private final String schema;

    private final String username;

    private final SSLConfig sslConfig;

    private PostgresqlConnectionConfiguration(String applicationName,
                                              @Nullable Duration connectTimeout,
                                              @Nullable String database,
                                              boolean forceBinary,
                                              String host,
                                              @Nullable Map<String, String> options,
                                              @Nullable String password,
                                              int port,
                                              @Nullable String schema,
                                              String username,
                                              SSLConfig sslConfig) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.connectTimeout = connectTimeout;
        this.database = database;
        this.forceBinary = forceBinary;
        this.host = Assert.requireNonNull(host, "host must not be null");
        this.options = options;
        this.password = sslConfig.getSslMode() != SSLMode.DISABLE
            ? password
            : Assert.requireNonNull(password, "password must not be null");
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

    @Override
    public String toString() {
        return "PostgresqlConnectionConfiguration{" +
            "applicationName='" + this.applicationName + '\'' +
            ", connectTimeout=" + this.connectTimeout +
            ", database='" + this.database + '\'' +
            ", forceBinary='" + this.forceBinary + '\'' +
            ", host='" + this.host + '\'' +
            ", options='" + this.options + '\'' +
            ", password='" + this.password + '\'' +
            ", port=" + this.port +
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

    String getHost() {
        return this.host;
    }

    @Nullable
    Map<String, String> getOptions() {
        return this.options;
    }

    @Nullable
    String getPassword() {
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

    public SSLConfig getSslConfig() {
        return sslConfig;
    }

    /**
     * A builder for {@link PostgresqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        // Note: defaultdir is ${user.home}/.postgresql/ in *nix systems and %appdata%/postgresql/ on windows
        private static final String DEFAULT_DIR = "${user.home}/.postgresql/";

        private String applicationName = "r2dbc-postgresql";

        private Duration connectTimeout;

        private String database;

        private boolean forceBinary = false;

        private String host;

        private Map<String, String> options;

        private String password;

        private int port = DEFAULT_PORT;

        private String schema;

        private String sslCert = null;

        private String sslHostnameVerifier = null;

        private String sslKey = null;

        private SSLMode sslMode = SSLMode.DISABLE;

        private CharSequence sslPassword = null;

        private String sslRootCert = DEFAULT_DIR + "root.crt";

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
         * Returns a configured {@link PostgresqlConnectionConfiguration}.
         *
         * @return a configured {@link PostgresqlConnectionConfiguration}
         */
        public PostgresqlConnectionConfiguration build() {
            SSLConfig sslConfig = this.createSslConfig();
            return new PostgresqlConnectionConfiguration(
                this.applicationName,
                this.connectTimeout,
                this.database,
                this.forceBinary,
                this.host,
                this.options,
                this.password,
                this.port,
                this.schema,
                this.username,
                sslConfig);
        }

        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
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
        public Builder password(@Nullable String password) {
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
         * Configure ssl cert.
         *
         * @param sslCert an X.509 certificate chain file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslCert(@Nullable String sslCert) {
            this.sslCert = Assert.requireFileExistsOrNull(sslCert, "sslCert must be null or exist");
            return this;
        }

        /**
         * Configure ssl HostnameVerifier.
         *
         * @param sslHostnameVerifier {@link javax.net.ssl.HostnameVerifier} implementation class name
         * @return this {@link Builder}
         */
        public Builder sslHostnameVerifier(@Nullable String sslHostnameVerifier) {
            this.sslHostnameVerifier = sslHostnameVerifier;
            return this;
        }

        /**
         * Configure ssl key.
         *
         * @param sslKey a PKCS#8 private key file in PEM format
         * @return this {@link Builder}
         */
        public Builder sslKey(@Nullable String sslKey) {
            this.sslKey = Assert.requireFileExistsOrNull(sslKey, "sslKey must be null or exist");
            return this;
        }

        /**
         * Configure ssl mode.
         *
         * @param sslMode
         * @return this {@link Builder}
         */
        public Builder sslMode(SSLMode sslMode) {
            this.sslMode = sslMode;
            return this;
        }

        /**
         * Configure ssl mode.
         *
         * @param sslMode
         * @return this {@link Builder}
         */
        public Builder sslMode(String sslMode) {
            this.sslMode = SSLMode.fromValue(sslMode);
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
         * Configure ssl root cert.
         *
         * @param sslRootCert
         * @return this {@link Builder}
         */
        public Builder sslRootCert(@Nullable String sslRootCert) {
            this.sslRootCert = Assert.requireFileExistsOrNull(sslRootCert, "sslKey must be null or exist");
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
                ", password='" + this.password + '\'' +
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
            HostnameVerifier hostnameVerifier = createSslHostVerifier();
            SslProvider sslProvider = createSslProvider();
            return new SSLConfig(this.sslMode, sslProvider, hostnameVerifier);
        }

        private HostnameVerifier createSslHostVerifier() {
            if (this.sslHostnameVerifier == null) {
                return PGHostnameVerifier.INSTANCE;
            }
            try {
                return (HostnameVerifier) Class.forName(this.sslHostnameVerifier).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }

        private SslProvider createSslProvider() {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            if (this.sslMode.verifyCertificate()) {
                String sslRootCert = Assert.requireNonNull(this.sslRootCert, "SSL mode " + this.sslMode + " requires sslRootCert parameter");
                sslContextBuilder.trustManager(new File(sslRootCert));
            } else {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }
            if (this.sslKey != null) {
                String sslCert = Assert.requireNonNull(this.sslCert, "SSL key param requires sslCert param to be present");
                String sslPassword = this.sslPassword == null ? null : this.sslPassword.toString();
                sslContextBuilder.keyManager(new File(sslCert), new File(this.sslKey), sslPassword);
            }
            return SslProvider.builder()
                .sslContext(sslContextBuilder)
                .defaultConfiguration(TCP)
                .build();
        }
    }
}
