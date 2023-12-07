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

import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.client.MultiHostConfiguration.ServerHost;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.extension.Extension;
import io.r2dbc.postgresql.util.LogLevel;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.AUTODETECT_EXTENSIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.COMPATIBILITY_MODE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.ERROR_RESPONSE_LOG_LEVEL;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.EXTENSIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FAILOVER_PROTOCOL;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FETCH_SIZE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FORCE_BINARY;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.HOST_RECHECK_TIME;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LEGACY_POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LOAD_BALANCE_HOSTS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LOCK_WAIT_TIMEOUT;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.OPTIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.PREFER_ATTACHED_BUFFERS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.PREPARED_STATEMENT_CACHE_QUERIES;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SOCKET;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_CERT;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_KEY;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_MODE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_ROOT_CERT;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.STATEMENT_TIMEOUT;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TARGET_SERVER_TYPE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TCP_KEEPALIVE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TCP_NODELAY;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TIME_ZONE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static io.r2dbc.spi.ConnectionFactoryOptions.builder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PostgresqlConnectionFactoryProvider}.
 */
final class PostgresqlConnectionFactoryProviderUnitTests {

    private final PostgresqlConnectionFactoryProvider provider = new PostgresqlConnectionFactoryProvider();

    @Test
    void doesNotSupportWithWrongDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, "test-driver")
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isFalse();
    }

    @Test
    void createFailsWithoutHost() {
        assertThatThrownBy(() -> this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void supportsWithoutHost() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void supportsWithoutPassword() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void returnsDriverIdentifier() {
        assertThat(this.provider.getDriver()).isEqualTo(POSTGRESQL_DRIVER);
    }

    @Test
    void supports() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void supportsPostgresDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .build())).isTrue();
    }

    @Test
    void supportsWithoutUser() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .build())).isTrue();
    }

    @Test
    void disableSsl() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL, false)
            .build());

        SSLConfig sslConfig = factory.getConfiguration().getSslConfig();

        assertThat(sslConfig.getSslMode()).isEqualTo(SSLMode.DISABLE);
    }

    @Test
    void enableSsl() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL, true)
            .build());

        SSLConfig sslConfig = factory.getConfiguration().getSslConfig();

        assertThat(sslConfig.getSslMode()).isEqualTo(SSLMode.VERIFY_FULL);
    }

    @Test
    void supportsSslCertificates() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL, true)
            .option(SSL_KEY, "client.key")
            .option(SSL_CERT, "client.crt")
            .option(SSL_ROOT_CERT, "server.crt")
            .build());

        assertThat(factory).isNotNull();
    }

    @Test
    void supportsSslCertificatesByClasspath() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL, true)
            .option(SSL_KEY, "client.key")
            .option(SSL_CERT, "client.crt")
            .option(SSL_ROOT_CERT, "server.crt")
            .build());

        assertThat(factory).isNotNull();
    }

    @Test
    void supportsSslMode() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("sslMode"), "DISABLE")
            .build());

        SSLConfig sslConfig = factory.getConfiguration().getSslConfig();

        assertThat(sslConfig.getSslMode()).isEqualTo(SSLMode.DISABLE);
    }

    @Test
    void supportsSslModeAlias() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("sslmode"), "require")
            .build());

        SSLConfig sslConfig = factory.getConfiguration().getSslConfig();

        assertThat(sslConfig.getSslMode()).isEqualTo(SSLMode.REQUIRE);
    }

    @Test
    void shouldCreateConnectionFactoryWithoutPassword() {
        assertThat(this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(USER, "test-user")
            .build())).isNotNull();
    }

    @Test
    void providerShouldConsiderFetchSize() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(FETCH_SIZE, 100)
            .build());

        assertThat(factory.getConfiguration().getFetchSize().applyAsInt("")).isEqualTo(100);
    }

    @Test
    void providerShouldConsiderFetchSizeAsString() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("fetchSize"), "100")
            .build());

        assertThat(factory.getConfiguration().getFetchSize().applyAsInt("")).isEqualTo(100);
    }

    @Test
    void providerShouldConsiderBinaryTransfer() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(FORCE_BINARY, true)
            .build());

        assertThat(factory.getConfiguration().isForceBinary()).isTrue();
    }

    @Test
    void providerShouldConsiderBinaryTransferWhenProvidedAsString() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("forceBinary"), "true")
            .build());

        assertThat(factory.getConfiguration().isForceBinary()).isTrue();
    }

    @Test
    void providerShouldConsiderCompatibilityMode() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(COMPATIBILITY_MODE, true)
            .build());

        assertThat(factory.getConfiguration().isCompatibilityMode()).isTrue();
    }

    @Test
    void providerShouldConsiderPreparedStatementCacheQueries() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(PREPARED_STATEMENT_CACHE_QUERIES, -2)
            .build());

        assertThat(factory.getConfiguration().getPreparedStatementCacheQueries()).isEqualTo(-2);
    }

    @Test
    void providerShouldConsiderPreparedStatementCacheQueriesWhenProvidedAsString() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("preparedStatementCacheQueries"), "5")
            .build());

        assertThat(factory.getConfiguration().getPreparedStatementCacheQueries()).isEqualTo(5);
    }

    @Test
    void providerShouldParseAndHandleConnectionParameters() {
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("lock_timeout", "5s");
        expectedOptions.put("statement_timeout", "6000");
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(OPTIONS, expectedOptions)
            .build());

        Map<String, String> actualOptions = factory.getConfiguration().getOptions();

        assertThat(actualOptions).isNotNull();
        assertThat(actualOptions).isEqualTo(expectedOptions);
    }

    @Test
    void providerShouldParseAndHandleLockStatementTimeouts() {
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("lock_timeout", "5000");
        expectedOptions.put("statement_timeout", "6000");
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(LOCK_WAIT_TIMEOUT, Duration.ofSeconds(5))
            .option(STATEMENT_TIMEOUT, Duration.ofSeconds(6))
            .build());

        Map<String, String> actualOptions = factory.getConfiguration().getOptions();

        assertThat(actualOptions).isNotNull();
        assertThat(actualOptions).isEqualTo(expectedOptions);
    }

    @Test
    void shouldConfigureAutodetectExtensions() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(AUTODETECT_EXTENSIONS, true)
            .build());

        assertThat(factory.getConfiguration().isAutodetectExtensions()).isTrue();

        factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(AUTODETECT_EXTENSIONS, false)
            .build());

        assertThat(factory.getConfiguration().isAutodetectExtensions()).isFalse();
    }

    @Test
    void shouldConfigureLogLevels() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(ERROR_RESPONSE_LOG_LEVEL, LogLevel.OFF)
            .option(Option.valueOf("noticeLogLevel"), "WARN")
            .build());

        assertThat(factory.getConfiguration())
            .hasFieldOrPropertyWithValue("errorResponseLogLevel", LogLevel.OFF)
            .hasFieldOrPropertyWithValue("noticeLogLevel", LogLevel.WARN);
    }

    @Test
    void shouldApplySslContextBuilderCustomizer() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(SSL_MODE, SSLMode.ALLOW)
            .option(SSL_CONTEXT_BUILDER_CUSTOMIZER, sslContextBuilder -> {
                throw new IllegalStateException("Works!");
            })
            .build());

        assertThatIllegalStateException().isThrownBy(() -> factory.getConfiguration().getSslConfig().getSslProvider().get()).withMessageContaining("Works!");
    }

    @Test
    void shouldConfigureTcpKeepAlive() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TCP_KEEPALIVE, true)
            .build());

        assertThat(factory.getConfiguration().isTcpKeepAlive()).isTrue();
    }

    @Test
    void shouldConfigureTcpNoDelay() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TCP_NODELAY, true)
            .build());

        assertThat(factory.getConfiguration().isTcpNoDelay()).isTrue();
    }

    @Test
    void shouldConfigureTimeZone() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(TIME_ZONE, TimeZone.getTimeZone("Europe/Amsterdam"))
            .build());

        assertThat(factory.getConfiguration().getTimeZone()).isEqualTo(TimeZone.getTimeZone("Europe/Amsterdam"));
    }

    @Test
    void shouldConfigureTimeZoneAsString() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(Option.valueOf("timeZone"), "Europe/Amsterdam")
            .build());

        assertThat(factory.getConfiguration().getTimeZone()).isEqualTo(TimeZone.getTimeZone("Europe/Amsterdam"));
    }

    @Test
    void shouldConnectUsingUnixDomainSocket() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(SOCKET, "/tmp/.s.PGSQL.5432")
            .option(USER, "postgres")
            .build());

        assertThat(factory.getConfiguration().getSingleHostConfiguration().isUseSocket()).isTrue();
        assertThat(factory.getConfiguration().getSingleHostConfiguration().getRequiredSocket()).isEqualTo("/tmp/.s.PGSQL.5432");
    }

    @Test
    void shouldConnectUsingMultiHostConfiguration() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(PROTOCOL, FAILOVER_PROTOCOL)
            .option(HOST, "host1:5433,host2:5432,host3")
            .option(USER, "postgres")
            .option(LOAD_BALANCE_HOSTS, true)
            .option(HOST_RECHECK_TIME, Duration.ofMillis(20000))
            .option(TARGET_SERVER_TYPE, MultiHostConnectionStrategy.TargetServerType.SECONDARY)
            .build());

        assertThat(factory.getConfiguration().getSingleHostConfiguration()).isNull();
        assertThat(factory.getConfiguration().getMultiHostConfiguration().isLoadBalanceHosts()).isEqualTo(true);
        assertThat(factory.getConfiguration().getMultiHostConfiguration().getHostRecheckTime()).isEqualTo(Duration.ofMillis(20000));
        assertThat(factory.getConfiguration().getMultiHostConfiguration().getTargetServerType()).isEqualTo(MultiHostConnectionStrategy.TargetServerType.SECONDARY);
        List<ServerHost> hosts = factory.getConfiguration().getMultiHostConfiguration().getHosts();
        assertThat(hosts).hasSize(3);
        assertThat(hosts.get(0)).usingRecursiveComparison().isEqualTo(new ServerHost("host1", 5433));
        assertThat(hosts.get(1)).usingRecursiveComparison().isEqualTo(new ServerHost("host2", 5432));
        assertThat(hosts.get(2)).usingRecursiveComparison().isEqualTo(new ServerHost("host3", 5432));
    }

    @Test
    void shouldConnectUsingMultiHostConfigurationFromUrl() {
        PostgresqlConnectionFactory factory = this.provider.create(ConnectionFactoryOptions.parse("r2dbc:postgresql:failover://user:foo@host1:5433,host2:5432,host3" +
            "?loadBalanceHosts=true&hostRecheckTime=20s&targetServerType=SECONdArY"));

        assertThat(factory.getConfiguration().getSingleHostConfiguration()).isNull();
        MultiHostConfiguration config = factory.getConfiguration().getRequiredMultiHostConfiguration();

        assertThat(config.isLoadBalanceHosts()).isEqualTo(true);
        assertThat(config.getHostRecheckTime()).isEqualTo(Duration.ofMillis(20000));
        assertThat(config.getTargetServerType()).isEqualTo(MultiHostConnectionStrategy.TargetServerType.SECONDARY);

        List<ServerHost> hosts = config.getHosts();
        assertThat(hosts).hasSize(3).containsExactly(new ServerHost("host1", 5433), new ServerHost("host2", 5432), new ServerHost("host3", 5432));
    }

    @Test
    void shouldParseOptionsProvidedAsString() {
        Option<String> options = Option.valueOf("options");
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(options, "search_path=public,private;default_tablespace=unknown")
            .build());

        assertThat(factory.getConfiguration().getOptions().get("search_path")).isEqualTo("public,private");
        assertThat(factory.getConfiguration().getOptions().get("default_tablespace")).isEqualTo("unknown");
    }

    @Test
    void shouldParseOptionsProvidedAsMap() {
        Option<Map<String, String>> options = Option.valueOf("options");

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("search_path", "public,private");
        optionsMap.put("default_tablespace", "unknown");

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(options, optionsMap)
            .build());

        assertThat(factory.getConfiguration().getOptions().get("search_path")).isEqualTo("public,private");
        assertThat(factory.getConfiguration().getOptions().get("default_tablespace")).isEqualTo("unknown");
    }

    @Test
    void shouldConfigurePreferAttachedBuffers() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(PREFER_ATTACHED_BUFFERS, true)
            .build());

        assertThat(factory.getConfiguration().isPreferAttachedBuffers()).isTrue();
    }

    @Test
    void shouldConfigureExtensions() {
        TestExtension testExtension1 = new TestExtension("extension-1");
        TestExtension testExtension2 = new TestExtension("extension-2");
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(PASSWORD, "test-password")
            .option(USER, "test-user")
            .option(EXTENSIONS, Arrays.asList(testExtension1, testExtension2))
            .build());

        assertThat(factory.getConfiguration().getExtensions()).containsExactly(testExtension1, testExtension2);
    }

    @Test
    void supportsUsernameAndPasswordSupplier() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(Option.valueOf("password"), (Supplier<String>) () -> "test-password")
            .option(Option.valueOf("user"), (Supplier<String>) () -> "test-user")
            .option(USER, "test-user")
            .build());

        StepVerifier.create(factory.getConfiguration().getPassword()).expectNext("test-password").verifyComplete();
        StepVerifier.create(factory.getConfiguration().getUsername()).expectNext("test-user").verifyComplete();
    }

    @Test
    void supportsUsernameAndPasswordPublisher() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, LEGACY_POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(Option.valueOf("password"), Mono.just("test-password"))
            .option(Option.valueOf("user"), Mono.just("test-user"))
            .option(USER, "test-user")
            .build());

        StepVerifier.create(factory.getConfiguration().getPassword()).expectNext("test-password").verifyComplete();
        StepVerifier.create(factory.getConfiguration().getUsername()).expectNext("test-user").verifyComplete();
    }

    private static class TestExtension implements Extension {

        private final String name;

        private TestExtension(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestExtension that = (TestExtension) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

    }

}
