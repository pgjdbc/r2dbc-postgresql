package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Map;

public interface ClientFactory {

    static ClientFactory getFactory(PostgresqlConnectionConfiguration configuration, ClientSupplier clientSupplier) {
        if (configuration.getSingleHostConfiguration() != null) {
            return new SingleHostClientFactory(configuration, clientSupplier);
        }
        if (configuration.getMultipleHostsConfiguration() != null) {
            return new MultipleHostsClientFactory(configuration, clientSupplier);
        }
        throw new IllegalArgumentException("Can't build client factory based on configuration " + configuration);
    }

    Mono<Client> create(@Nullable Map<String, String> options);
}
