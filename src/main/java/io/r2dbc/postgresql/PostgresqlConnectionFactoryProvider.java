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

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
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
     * Force binary transfer.
     */
    public static final Option<Boolean> FORCE_BINARY = Option.valueOf("forceBinary");

    /**
     * Load balance hosts.
     */
    public static final Option<Boolean> LOAD_BALANCE_HOSTS = Option.valueOf("loadBalanceHosts");

    /**
     * Host status recheck time im ms.
     */
    public static final Option<Integer> HOST_RECHECK_TIME = Option.valueOf("hostRecheckTime");

    /**
     * Target server type. Allowed values: any, master, secondary, preferSecondary.
     */
    public static final Option<TargetServerType> TARGET_SERVER_TYPE = Option.valueOf("targetServerType");

    /**
     * Driver option value.
     */
    public static final String POSTGRESQL_DRIVER = "postgresql";

    /**
     * Failover driver protocol.
     */
    public static final String FAILOVER_PROTOCOL = "failover";

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
        return new PostgresqlConnectionFactory(createConfiguration(connectionFactoryOptions));
    }

    static PostgresqlConnectionConfiguration createConfiguration(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        boolean tcp;
        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder();

        String applicationName = connectionFactoryOptions.getValue(APPLICATION_NAME);
        if (applicationName != null) {
            builder.applicationName(applicationName);
        }

        Object autodetectExtensions = connectionFactoryOptions.getValue(AUTODETECT_EXTENSIONS);
        if (autodetectExtensions != null) {
            if (autodetectExtensions instanceof Boolean) {
                builder.autodetectExtensions((Boolean) autodetectExtensions);
            } else {
                builder.autodetectExtensions(Boolean.parseBoolean(autodetectExtensions.toString()));
            }
        }

        builder.connectTimeout(connectionFactoryOptions.getValue(CONNECT_TIMEOUT));
        builder.database(connectionFactoryOptions.getValue(DATABASE));

        if (FAILOVER_PROTOCOL.equals(connectionFactoryOptions.getValue(PROTOCOL))) {
            if (connectionFactoryOptions.hasOption(HOST_RECHECK_TIME)) {
                builder.hostRecheckTime(connectionFactoryOptions.getRequiredValue(HOST_RECHECK_TIME));
            }
            if (connectionFactoryOptions.hasOption(LOAD_BALANCE_HOSTS)) {
                Object loadBalanceHosts = connectionFactoryOptions.getRequiredValue(LOAD_BALANCE_HOSTS);
                if (loadBalanceHosts instanceof Boolean) {
                    builder.loadBalanceHosts((Boolean) loadBalanceHosts);
                } else {
                    builder.loadBalanceHosts(Boolean.parseBoolean(loadBalanceHosts.toString()));
                }
            }
            if (connectionFactoryOptions.hasOption(TARGET_SERVER_TYPE)) {
                Object targetServerType = connectionFactoryOptions.getRequiredValue(TARGET_SERVER_TYPE);
                if (targetServerType instanceof TargetServerType) {
                    builder.targetServerType((TargetServerType) targetServerType);
                } else {
                    builder.targetServerType(TargetServerType.fromValue(targetServerType.toString()));
                }
            }
            String hosts = connectionFactoryOptions.getRequiredValue(HOST);
            String[] hostsArray = hosts.split(",");
            for (String host : hostsArray) {
                String[] hostParts = host.split(":");
                if (hostParts.length == 1) {
                    builder.addHost(hostParts[0]);
                } else {
                    int port = Integer.parseInt(hostParts[1]);
                    builder.addHost(hostParts[0], port);
                }
            }
            tcp = true;
        } else {
            if (connectionFactoryOptions.hasOption(SOCKET)) {
                tcp = false;
                builder.socket(connectionFactoryOptions.getRequiredValue(SOCKET));
            } else {
                tcp = true;
                builder.host(connectionFactoryOptions.getRequiredValue(HOST));
            }
            Integer port = connectionFactoryOptions.getValue(PORT);
            if (port != null) {
                builder.port(port);
            }
        }


        builder.password(connectionFactoryOptions.getValue(PASSWORD));
        builder.schema(connectionFactoryOptions.getValue(SCHEMA));
        builder.username(connectionFactoryOptions.getRequiredValue(USER));


        Boolean forceBinary = connectionFactoryOptions.getValue(FORCE_BINARY);

        if (forceBinary != null) {
            builder.forceBinary(forceBinary);
        }

        Map<String, String> options = connectionFactoryOptions.getValue(OPTIONS);
        if (options != null) {
            builder.options(options);
        }

        if (tcp) {
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

            if (connectionFactoryOptions.hasOption(SSL_CONTEXT_BUILDER_CUSTOMIZER)) {
                builder.sslContextBuilderCustomizer(connectionFactoryOptions.getRequiredValue(SSL_CONTEXT_BUILDER_CUSTOMIZER));
            }
        }

        return builder.build();
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
