/*
 * Copyright 2017-2020 the original author or authors.
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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a PostgreSQL database.
 */
public final class PostgresqlConnectionFactory implements ConnectionFactory {

    private static final String REPLICATION_OPTION = "replication";

    private static final String REPLICATION_DATABASE = "database";

    private final Function<ConnectionSettings, Mono<? extends Client>> clientFactory;

    private final PostgresqlConnectionConfiguration configuration;

    private final SocketAddress endpoint;

    private final Extensions extensions;

    /**
     * Create a new connection factory.
     *
     * @param configuration the configuration to use connections
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public PostgresqlConnectionFactory(PostgresqlConnectionConfiguration configuration) {
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        this.endpoint = createSocketAddress(configuration);
        this.clientFactory = settings -> ReactorNettyClient.connect(this.endpoint, settings).cast(Client.class);
        this.extensions = getExtensions(configuration);
    }

    PostgresqlConnectionFactory(Function<ConnectionSettings, Mono<? extends Client>> clientFactory, PostgresqlConnectionConfiguration configuration) {
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        this.endpoint = createSocketAddress(configuration);
        this.clientFactory = Assert.requireNonNull(clientFactory, "clientFactory must not be null");
        this.extensions = getExtensions(configuration);
    }

    private static SocketAddress createSocketAddress(PostgresqlConnectionConfiguration configuration) {

        if (!configuration.isUseSocket()) {
            return InetSocketAddress.createUnresolved(configuration.getRequiredHost(), configuration.getPort());
        }

        return DomainSocketFactory.getDomainSocketAddress(configuration);
    }

    private static Extensions getExtensions(PostgresqlConnectionConfiguration configuration) {
        Extensions extensions = Extensions.from(configuration.getExtensions());

        if (configuration.isAutodetectExtensions()) {
            extensions = extensions.mergeWith(Extensions.autodetect());
        }

        return extensions;
    }

    @Override
    public Mono<io.r2dbc.postgresql.api.PostgresqlConnection> create() {

        if (isReplicationConnection()) {
            throw new UnsupportedOperationException("Cannot create replication connection through create(). Use replication() method instead.");
        }

        return doCreateConnection(false, this.configuration.getOptions()).cast(io.r2dbc.postgresql.api.PostgresqlConnection.class);
    }

    /**
     * Create a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     *
     * @return a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     */
    public Mono<io.r2dbc.postgresql.api.PostgresqlReplicationConnection> replication() {

        Map<String, String> options = new LinkedHashMap<>(this.configuration.getOptions());
        options.put(REPLICATION_OPTION, REPLICATION_DATABASE);

        return doCreateConnection(true, options).map(DefaultPostgresqlReplicationConnection::new);
    }

    private Mono<PostgresqlConnection> doCreateConnection(boolean forReplication, @Nullable Map<String, String> options) {

        SSLConfig sslConfig = this.configuration.getSslConfig();
        ConnectionSettings connectionSettings = this.configuration.getConnectionSettings();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.tryConnectWithConfig(connectionSettings, options)
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW),
                e -> this.tryConnectWithConfig(connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(SSLMode.REQUIRE))), options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER),
                e -> this.tryConnectWithConfig(connectionSettings.mutate(builder -> builder.sslConfig(sslConfig.mutateMode(SSLMode.DISABLE))), options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .flatMap(client -> {

                DefaultCodecs codecs = new DefaultCodecs(client.getByteBufAllocator(), this.configuration.isPreferAttachedBuffers());
                StatementCache statementCache = StatementCache.fromPreparedStatementCacheQueries(client, this.configuration.getPreparedStatementCacheQueries());

                // early connection object to retrieve initialization details
                PostgresqlConnection earlyConnection = new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, IsolationLevel.READ_COMMITTED,
                    this.configuration);

                Mono<IsolationLevel> isolationLevelMono = Mono.just(IsolationLevel.READ_COMMITTED);
                if (!forReplication) {
                    isolationLevelMono = getIsolationLevel(earlyConnection);
                }
                return isolationLevelMono
                    // actual connection to be used
                    .map(isolationLevel -> new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, statementCache, isolationLevel, this.configuration))
                    .delayUntil(connection -> {
                        return prepareConnection(connection, client.getByteBufAllocator(), codecs, forReplication);
                    })
                    .onErrorResume(throwable -> this.closeWithError(client, throwable));
            }).onErrorMap(this::cannotConnect);
    }

    private boolean isReplicationConnection() {
        Map<String, String> options = this.configuration.getOptions();
        return REPLICATION_DATABASE.equalsIgnoreCase(options.get(REPLICATION_OPTION));
    }

    private Mono<Client> tryConnectWithConfig(ConnectionSettings settings, @Nullable Map<String, String> options) {
        return this.clientFactory.apply(settings)
            .delayUntil(client -> StartupMessageFlow
                .exchange(this.configuration.getApplicationName(), this::getAuthenticationHandler, client, this.configuration.getDatabase(), this.configuration.getUsername(),
                    options)
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse))
            .cast(Client.class);
    }

    private Publisher<?> prepareConnection(PostgresqlConnection connection, ByteBufAllocator byteBufAllocator, DefaultCodecs codecs, boolean forReplication) {

        List<Publisher<?>> publishers = new ArrayList<>();

        if (!forReplication) {
            this.extensions.forEach(CodecRegistrar.class, it -> {
                publishers.add(it.register(connection, byteBufAllocator, codecs));
            });
        }

        return Flux.concat(publishers).then();
    }

    private Mono<PostgresqlConnection> closeWithError(Client client, Throwable throwable) {
        return client.close().then(Mono.error(throwable));
    }

    private Throwable cannotConnect(Throwable throwable) {

        if (throwable instanceof R2dbcException) {
            return throwable;
        }

        return new PostgresConnectionException(
            String.format("Cannot connect to %s", this.endpoint), throwable
        );
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return PostgresqlConnectionFactoryMetadata.INSTANCE;
    }

    PostgresqlConnectionConfiguration getConfiguration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return "PostgresqlConnectionFactory{" +
            "clientFactory=" + this.clientFactory +
            ", configuration=" + this.configuration +
            ", extensions=" + this.extensions +
            '}';
    }

    private AuthenticationHandler getAuthenticationHandler(AuthenticationMessage message) {
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

    private Mono<IsolationLevel> getIsolationLevel(io.r2dbc.postgresql.api.PostgresqlConnection connection) {
        return connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                String level = row.get(0, String.class);

                if (level == null) {
                    return IsolationLevel.READ_COMMITTED; // Best guess.
                }

                return IsolationLevel.valueOf(level.toUpperCase(Locale.US));
            })).defaultIfEmpty(IsolationLevel.READ_COMMITTED).last();
    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException {

        public PostgresConnectionException(String msg, @Nullable Throwable cause) {
            super(msg, cause);
        }

    }

    static class DomainSocketFactory {

        private static SocketAddress getDomainSocketAddress(PostgresqlConnectionConfiguration configuration) {
            return new DomainSocketAddress(configuration.getRequiredSocket());
        }

    }

}
