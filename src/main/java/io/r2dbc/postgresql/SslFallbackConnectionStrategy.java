package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class SslFallbackConnectionStrategy implements ConnectionStrategy.ComposableConnectionStrategy {

    private final PostgresqlConnectionConfiguration configuration;

    private final ComposableConnectionStrategy connectionStrategy;

    SslFallbackConnectionStrategy(PostgresqlConnectionConfiguration configuration, ComposableConnectionStrategy connectionStrategy) {
        this.configuration = configuration;
        this.connectionStrategy = connectionStrategy;
    }

    @Override
    public Mono<Client> connect() {
        SSLConfig sslConfig = this.configuration.getSslConfig();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.connectionStrategy.connect()
            .onErrorResume(isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW), fallback(SSLMode.REQUIRE))
            .onErrorResume(isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER), fallback(SSLMode.DISABLE));
    }

    private Function<Throwable, Mono<Client>> fallback(SSLMode sslMode) {
        ConnectionSettings connectionSettings = this.configuration.getConnectionSettings();
        SSLConfig sslConfig = this.configuration.getSslConfig();
        return e -> this.connectionStrategy.withConnectionSettings(connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(sslMode))))
            .connect()
            .onErrorResume(sslAuthError -> {
                e.addSuppressed(sslAuthError);
                return Mono.error(e);
            });
    }

    @Override
    public ComposableConnectionStrategy withAddress(SocketAddress address) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withAddress(address));
    }

    @Override
    public ComposableConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withConnectionSettings(connectionSettings));
    }

    @Override
    public ComposableConnectionStrategy withOptions(Map<String, String> options) {
        return new SslFallbackConnectionStrategy(this.configuration, this.connectionStrategy.withOptions(options));
    }

}
