/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.util.LogLevel;
import org.junit.jupiter.api.Test;
import reactor.netty.resources.ConnectionProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ConnectionSettings}.
 */
final class ConnectionSettingsUnitTests {

    @Test
    void builderNoConnectionProvider() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionSettings.builder().connectionProvider(null))
            .withMessage("connectionProvider must not be null");
    }

    @Test
    void builderNoerrorResponseLogLevel() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionSettings.builder().errorResponseLogLevel(null))
            .withMessage("errorResponseLogLevel must not be null");
    }

    @Test
    void builderNoNoticeLogLevel() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionSettings.builder().noticeLogLevel(null))
            .withMessage("noticeLogLevel must not be null");
    }

    @Test
    void builderNoSslConfig() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionSettings.builder().sslConfig(null))
            .withMessage("sslConfig must not be null");
    }

    @Test
    void build() {

        ConnectionSettings connectionSettings =
            ConnectionSettings.builder().connectionProvider(ConnectionProvider.newConnection()).errorResponseLogLevel(LogLevel.OFF).noticeLogLevel(LogLevel.ERROR).sslConfig(new SSLConfig(SSLMode.DISABLE, null, null)).build();

        assertThat(connectionSettings)
            .hasFieldOrPropertyWithValue("connectionProvider", ConnectionProvider.newConnection())
            .hasFieldOrPropertyWithValue("errorResponseLogLevel", LogLevel.OFF)
            .hasFieldOrPropertyWithValue("noticeLogLevel", LogLevel.ERROR)
            .hasFieldOrProperty("sslConfig");
    }

    @Test
    void mutate() {

        ConnectionProvider foo = ConnectionProvider.builder("foo").build();
        ConnectionSettings connectionSettings =
            ConnectionSettings.builder().connectionProvider(ConnectionProvider.newConnection()).errorResponseLogLevel(LogLevel.OFF).noticeLogLevel(LogLevel.ERROR).sslConfig(new SSLConfig(SSLMode.DISABLE, null, null)).build();

        ConnectionSettings mutated = connectionSettings.mutate(builder -> builder.connectionProvider(foo));

        assertThat(mutated)
            .hasFieldOrPropertyWithValue("connectionProvider", foo)
            .hasFieldOrPropertyWithValue("errorResponseLogLevel", LogLevel.OFF)
            .hasFieldOrPropertyWithValue("noticeLogLevel", LogLevel.ERROR)
            .hasFieldOrProperty("sslConfig");
    }

}
