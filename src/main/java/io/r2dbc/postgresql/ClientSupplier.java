package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.SSLConfig;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.time.Duration;

public interface ClientSupplier {

    Mono<Client> connect(SocketAddress endpoint, @Nullable Duration connectTimeout, SSLConfig sslConfig);
}
