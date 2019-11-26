package io.r2dbc.postgresql;

import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.SingleHostConfiguration;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

class SingleHostClientFactory extends ClientFactoryBase {

    private final SocketAddress endpoint;

    protected SingleHostClientFactory(PostgresqlConnectionConfiguration configuration, ClientSupplier clientSupplier) {
        super(configuration, clientSupplier);
        this.endpoint = SingleHostClientFactory.createSocketAddress(configuration.getSingleHostConfiguration());
    }

    @Override
    public Mono<? extends Client> create(@Nullable Map<String, String> options) {
        return this.tryConnectToEndpoint(this.endpoint, options);
    }


    protected static SocketAddress createSocketAddress(SingleHostConfiguration configuration) {
        if (!configuration.isUseSocket()) {
            return InetSocketAddress.createUnresolved(configuration.getRequiredHost(), configuration.getPort());
        }

        if (configuration.isUseSocket()) {
            return new DomainSocketAddress(configuration.getRequiredSocket());
        }

        throw new IllegalArgumentException("Cannot create SocketAddress for " + configuration);
    }

}
