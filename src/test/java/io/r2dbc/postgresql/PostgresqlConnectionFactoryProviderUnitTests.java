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

import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.AUTODETECT_EXTENSIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.COMPATIBILITY_MODE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FETCH_SIZE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.FORCE_BINARY;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.LEGACY_POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.OPTIONS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.PREFER_ATTACHED_BUFFERS;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.PREPARED_STATEMENT_CACHE_QUERIES;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SOCKET;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.SSL_MODE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TCP_KEEPALIVE;
import static io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider.TCP_NODELAY;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
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
    void shouldConnectUsingUnixDomainSocket() {

        PostgresqlConnectionFactory factory = this.provider.create(builder()
            .option(DRIVER, POSTGRESQL_DRIVER)
            .option(SOCKET, "/tmp/.s.PGSQL.5432")
            .option(USER, "postgres")
            .build());

        assertThat(factory.getConfiguration().isUseSocket()).isTrue();
        assertThat(factory.getConfiguration().getRequiredSocket()).isEqualTo("/tmp/.s.PGSQL.5432");
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

}
