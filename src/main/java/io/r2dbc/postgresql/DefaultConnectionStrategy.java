package io.r2dbc.postgresql;

import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;

public class DefaultConnectionStrategy implements ConnectionStrategy.ComposableConnectionStrategy {

    private final SocketAddress address;

    private final ClientSupplier clientSupplier;

    private final PostgresqlConnectionConfiguration configuration;

    private final ConnectionSettings connectionSettings;

    private final Map<String, String> options;

    DefaultConnectionStrategy(
        @Nullable SocketAddress address,
        ClientSupplier clientSupplier,
        PostgresqlConnectionConfiguration configuration,
        ConnectionSettings connectionSettings,
        @Nullable Map<String, String> options
    ) {
        this.address = address;
        this.clientSupplier = clientSupplier;
        this.configuration = configuration;
        this.connectionSettings = connectionSettings;
        this.options = options;
    }

    @Override
    public Mono<Client> connect() {
        Assert.requireNonNull(this.address, "address must not be null");
        return this.clientSupplier.connect(this.address, this.connectionSettings)
            .delayUntil(client -> StartupMessageFlow
                .exchange(this.configuration.getApplicationName(), this::getAuthenticationHandler, client, this.configuration.getDatabase(), this.configuration.getUsername(), this.options)
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse));
    }

    @Override
    public ComposableConnectionStrategy withAddress(SocketAddress address) {
        return new DefaultConnectionStrategy(address, this.clientSupplier, this.configuration, this.connectionSettings, this.options);
    }

    @Override
    public ComposableConnectionStrategy withConnectionSettings(ConnectionSettings connectionSettings) {
        return new DefaultConnectionStrategy(this.address, this.clientSupplier, this.configuration, connectionSettings, this.options);
    }

    @Override
    public ComposableConnectionStrategy withOptions(Map<String, String> options) {
        return new DefaultConnectionStrategy(this.address, this.clientSupplier, this.configuration, this.connectionSettings, options);
    }

    protected AuthenticationHandler getAuthenticationHandler(AuthenticationMessage message) {
        if (PasswordAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new PasswordAuthenticationHandler(password, this.configuration.getUsername());
        } else if (SASLAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new SASLAuthenticationHandler(password, this.configuration.getUsername());
        } else {
            throw new IllegalStateException(String.format("Unable to provide AuthenticationHandler capable of handling %s", message));
        }
    }

}
