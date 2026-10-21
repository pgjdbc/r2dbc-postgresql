/*
 * Copyright 2026 the original author or authors.
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

import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import org.junit.jupiter.api.Test;
import reactor.netty.resources.LoopResources;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReactorNettyClient}.
 */
final class ReactorNettyClientUnitTests {

    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 5432);

    @Test
    void sharesResolverAcrossClients() {

        ConnectionSettings settings = ConnectionSettings.builder().build();

        assertThat(ReactorNettyClient.createTcpClient(address, settings).configuration().resolver()).isInstanceOf(RoundRobinDnsAddressResolverGroup.class)
            .isSameAs(ReactorNettyClient.createTcpClient(address, settings).configuration().resolver());
    }

    @Test
    void sharesResolverPerLoopResources() {

        LoopResources loopResources = LoopResources.create("test", 1, true);

        try {
            ConnectionSettings settings = ConnectionSettings.builder().loopResources(loopResources).build();

            assertThat(ReactorNettyClient.createTcpClient(address, settings).configuration().resolver())
                .isSameAs(ReactorNettyClient.createTcpClient(address, settings).configuration().resolver())
                .isNotSameAs(ReactorNettyClient.createTcpClient(address, ConnectionSettings.builder().build()).configuration().resolver());
        } finally {
            loopResources.dispose();
        }
    }

}
