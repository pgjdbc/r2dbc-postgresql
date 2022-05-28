package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;

public interface ClientSupplier {

    Mono<Client> connect(SocketAddress endpoint, ConnectionSettings settings);

}
