package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Function;

public interface ConnectionStrategy {

    Mono<Client> connect();

    ConnectionStrategy withOptions(Map<String, String> options);

    interface ComposableConnectionStrategy extends ConnectionStrategy {

        default  <T extends ConnectionStrategy> T chainIf(boolean guard, Function<ComposableConnectionStrategy, T> nextStrategyProvider, Class<T> klass) {
            return guard ? nextStrategyProvider.apply(this) : klass.cast(this);
        }

        ComposableConnectionStrategy withAddress(SocketAddress address);

        ComposableConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings);

        ComposableConnectionStrategy withOptions(Map<String, String> options);

    }

}
