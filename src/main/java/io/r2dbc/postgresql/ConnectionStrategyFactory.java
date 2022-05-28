package io.r2dbc.postgresql;

import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.SingleHostConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ConnectionStrategyFactory {

    public static ConnectionStrategy getConnectionStrategy(ClientSupplier clientSupplier, PostgresqlConnectionConfiguration configuration) {
        SingleHostConfiguration singleHostConfiguration = configuration.getSingleHostConfiguration();
        MultiHostConfiguration multiHostConfiguration = configuration.getMultiHostConfiguration();
        SSLConfig sslConfig = configuration.getSslConfig();
        SocketAddress address = singleHostConfiguration != null ? createSocketAddress(singleHostConfiguration) : null;
        return new DefaultConnectionStrategy(address, clientSupplier, configuration, configuration.getConnectionSettings(), configuration.getOptions())
            .chainIf(!SSLMode.DISABLE.equals(sslConfig.getSslMode()), strategy -> new SslFallbackConnectionStrategy(configuration, strategy), ConnectionStrategy.ComposableConnectionStrategy.class)
            .chainIf(multiHostConfiguration != null, strategy -> new MultiHostConnectionStrategy(createSocketAddress(multiHostConfiguration), configuration, strategy), ConnectionStrategy.class);
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
