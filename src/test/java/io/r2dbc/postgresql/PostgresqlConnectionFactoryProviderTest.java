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

import io.r2dbc.postgresql.client.MultipleHostsConfiguration;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.AUTODETECT_EXTENSIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FAILOVER_PROTOCOL;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FORCE_BINARY;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.HOST_RECHECK_TIME;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LEGACY_POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LOAD_BALANCE_HOSTS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.OPTIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.PREPARED_STATEMENT_CACHE_QUERIES;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SOCKET;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_MODE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TARGET_SERVER_TYPE;
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

final class PostgresqlConnectionFactoryProviderTest {

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
    void supportsSsl() {
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
    void shouldCreateConnectionFactoryWithoutPassword() {
        assertThat(this.provider.create(ConnectionFactoryOptions.builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(HOST, "test-host")
            .option(USER, "test-user")
            .build())).isNotNull();
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
    void testFailoverConfiguration() {
        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(PROTOCOL, FAILOVER_PROTOCOL)
            .option(HOST, "host1:5433,host2:5432,host3")
            .option(USER, "postgres")
            .option(LOAD_BALANCE_HOSTS, true)
            .option(HOST_RECHECK_TIME, 20000)
            .option(TARGET_SERVER_TYPE, TargetServerType.SECONDARY)
            .build());

        assertThat(factory.getConfiguration().getSingleHostConfiguration()).isNull();
        assertThat(factory.getConfiguration().getMultipleHostsConfiguration().isLoadBalanceHosts()).isEqualTo(true);
        assertThat(factory.getConfiguration().getMultipleHostsConfiguration().getHostRecheckTime()).isEqualTo(20000);
        assertThat(factory.getConfiguration().getMultipleHostsConfiguration().getTargetServerType()).isEqualTo(TargetServerType.SECONDARY);
        List<MultipleHostsConfiguration.ServerHost> hosts = factory.getConfiguration().getMultipleHostsConfiguration().getHosts();
        assertThat(hosts).hasSize(3);
        assertThat(hosts.get(0)).isEqualToComparingFieldByField(new MultipleHostsConfiguration.ServerHost("host1", 5433));
        assertThat(hosts.get(1)).isEqualToComparingFieldByField(new MultipleHostsConfiguration.ServerHost("host2", 5432));
        assertThat(hosts.get(2)).isEqualToComparingFieldByField(new MultipleHostsConfiguration.ServerHost("host3", 5432));
    }
}
