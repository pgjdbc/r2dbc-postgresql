/*
 * Copyright 2022 the original author or authors.
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

import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.SingleHostConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory methods to obtain a {@link ConnectionStrategy} object.
 *
 * @since 1.0
 */
final class ConnectionStrategyFactory {

    /**
     * Create a {@link ConnectionStrategy} that is able to connect to the specified {@link PostgresqlConnectionConfiguration configuration}.
     *
     * @param connectionFunction the raw connection function to use to create a {@link Client}. The connection function is enhanced during the connect phase to perform a handshake with the database.
     * @param configuration      the configuration object
     * @return the connection strategy to use.
     */
    public static ConnectionStrategy getConnectionStrategy(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration, ConnectionSettings connectionSettings) {
        return doGetConnectionStrategy(new SingleHostConnectionFunction(connectionFunction, configuration), configuration, connectionSettings);
    }

    private static ConnectionStrategy doGetConnectionStrategy(ConnectionFunction connectionFunction, PostgresqlConnectionConfiguration configuration, ConnectionSettings connectionSettings) {

        SSLConfig sslConfig = configuration.getSslConfig();
        if (!SSLMode.DISABLE.equals(sslConfig.getSslMode())) {
            connectionFunction = new SslFallbackConnectionFunction(sslConfig, connectionFunction);
        }

        MultiHostConfiguration multiHostConfiguration = configuration.getMultiHostConfiguration();
        if (multiHostConfiguration != null) {
            return new MultiHostConnectionStrategy(connectionFunction, createSocketAddress(multiHostConfiguration), configuration, connectionSettings);
        }

        return new SingleHostConnectionStrategy(connectionFunction, createSocketAddress(configuration.getRequiredSingleHostConfiguration()), connectionSettings);
    }

    private static SocketAddress createSocketAddress(SingleHostConfiguration configuration) {
        if (!configuration.isUseSocket()) {
            return InetSocketAddress.createUnresolved(configuration.getRequiredHost(), configuration.getPort());
        }

        return DomainSocketFactory.getDomainSocketAddress(configuration);
    }

    static class DomainSocketFactory {

        private static SocketAddress getDomainSocketAddress(SingleHostConfiguration configuration) {
            return new DomainSocketAddress(configuration.getRequiredSocket());
        }

    }

    private static List<SocketAddress> createSocketAddress(MultiHostConfiguration configuration) {

        List<SocketAddress> addressList = new ArrayList<>(configuration.getHosts().size());

        for (MultiHostConfiguration.ServerHost host : configuration.getHosts()) {
            addressList.add(InetSocketAddress.createUnresolved(host.getHost(), host.getPort()));
        }

        return addressList;
    }

}
