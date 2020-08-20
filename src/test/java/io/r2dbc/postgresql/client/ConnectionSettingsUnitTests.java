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
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
        LoopResources loopResources = mock(LoopResources.class);
        ConnectionSettings connectionSettings =
            ConnectionSettings.builder().connectionProvider(ConnectionProvider.newConnection())
                .tcpLoopResources(loopResources)
                .errorResponseLogLevel(LogLevel.OFF).noticeLogLevel(LogLevel.ERROR)
                .sslConfig(new SSLConfig(SSLMode.DISABLE, null, null)).build();

        assertThat(connectionSettings)
            .hasFieldOrPropertyWithValue("connectionProvider", ConnectionProvider.newConnection())
            .hasFieldOrPropertyWithValue("tcpLoopResources", loopResources)
            .hasFieldOrPropertyWithValue("errorResponseLogLevel", LogLevel.OFF)
            .hasFieldOrPropertyWithValue("noticeLogLevel", LogLevel.ERROR)
            .hasFieldOrProperty("sslConfig");
    }

    @Test
    void mutate() {
        ConnectionProvider foo = ConnectionProvider.builder("foo").build();
        ConnectionSettings connectionSettings =
            ConnectionSettings.builder().connectionProvider(ConnectionProvider.newConnection())
                .tcpLoopResources(TcpResources.get())
                .errorResponseLogLevel(LogLevel.OFF).noticeLogLevel(LogLevel.ERROR)
                .connectTimeout(Duration.ofSeconds(30)).tcpKeepAlive(true).tcpNoDelay(true)
                .sslConfig(new SSLConfig(SSLMode.DISABLE, null, null)).build();

        ConnectionSettings mutated = connectionSettings.mutate(builder -> builder.connectionProvider(foo));

        assertThat(mutated)
            .hasFieldOrPropertyWithValue("connectionProvider", foo)
            .hasFieldOrPropertyWithValue("tcpLoopResources", TcpResources.get())
            .hasFieldOrPropertyWithValue("errorResponseLogLevel", LogLevel.OFF)
            .hasFieldOrPropertyWithValue("noticeLogLevel", LogLevel.ERROR)
            .hasFieldOrPropertyWithValue("connectTimeout", Duration.ofSeconds(30))
            .hasFieldOrPropertyWithValue("tcpKeepAlive", true)
            .hasFieldOrPropertyWithValue("tcpNoDelay", true)
            .hasFieldOrProperty("sslConfig");
    }

    @Test
    void mutateLoopResources() {
        LoopResources loopResources1 = mock(LoopResources.class);
        LoopResources loopResources2 = mock(LoopResources.class);
        ConnectionSettings connectionSettings = ConnectionSettings.builder().tcpLoopResources(loopResources1).build();

        ConnectionSettings mutated = connectionSettings.mutate(builder -> builder.tcpLoopResources(loopResources2));

        assertThat(mutated).hasFieldOrPropertyWithValue("tcpLoopResources", loopResources2);
    }

}
