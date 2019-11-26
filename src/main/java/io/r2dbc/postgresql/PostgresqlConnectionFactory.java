/*
 * Copyright 2017-2019 the original author or authors.
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
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a PostgreSQL database.
 */
public final class PostgresqlConnectionFactory implements ConnectionFactory {

    private static final String REPLICATION_OPTION = "replication";

    private static final String REPLICATION_DATABASE = "database";

    private final Function<Map<String, String>, Mono<? extends Client>> clientFactory;

    private final PostgresqlConnectionConfiguration configuration;

    private final Extensions extensions;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use connections
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public PostgresqlConnectionFactory(PostgresqlConnectionConfiguration configuration) {
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        this.clientFactory = new ClientFactory(configuration,
            (endpoint, connectTimeout, sslConfig) -> ReactorNettyClient.connect(ConnectionProvider.newConnection(), endpoint, connectTimeout, sslConfig)
                .cast(Client.class));
        this.extensions = getExtensions(configuration);
    }

    PostgresqlConnectionFactory(ClientFactory.ConnectionSupplier connectionSupplier, PostgresqlConnectionConfiguration configuration) {
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
        Assert.requireNonNull(connectionSupplier, "connectionSupplier must not be null");
        this.clientFactory = new ClientFactory(configuration, connectionSupplier);
        this.extensions = getExtensions(configuration);
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
     * Creates a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     *
     * @return a new {@link io.r2dbc.postgresql.api.PostgresqlReplicationConnection} for interaction with replication streams.
     */
    public Mono<io.r2dbc.postgresql.api.PostgresqlReplicationConnection> replication() {

        Map<String, String> options = this.configuration.getOptions();
        if (options == null) {
            options = new HashMap<>();
        } else {
            options = new HashMap<>(options);
        }

        options.put(REPLICATION_OPTION, REPLICATION_DATABASE);

        return doCreateConnection(true, options).map(DefaultPostgresqlReplicationConnection::new);
    }

    private Throwable cannotConnect(Throwable throwable) {

        if (throwable instanceof R2dbcException) {
            return throwable;
        }

        return new PostgresConnectionException(
            String.format("Cannot connect to %s", "TODO"), throwable // TODO
        );
    }

    private boolean isReplicationConnection() {
        Map<String, String> options = this.configuration.getOptions();
        return options != null && REPLICATION_DATABASE.equalsIgnoreCase(options.get(REPLICATION_OPTION));
    }

    private Publisher<?> prepareConnection(PostgresqlConnection connection, ByteBufAllocator byteBufAllocator, DefaultCodecs codecs) {

        List<Publisher<?>> publishers = new ArrayList<>();
        publishers.add(setSchema(connection));

        this.extensions.forEach(CodecRegistrar.class, it -> {
            publishers.add(it.register(connection, byteBufAllocator, codecs));
        });

        return Flux.concat(publishers).then();
    }

    private Mono<PostgresqlConnection> closeWithError(Client client, Throwable throwable) {
        return client.close().then(Mono.error(throwable));
    }

    private Mono<PostgresqlConnection> doCreateConnection(boolean forReplication, @Nullable Map<String, String> options) {
        return this.clientFactory.apply(options)
            .flatMap(client -> {
                DefaultCodecs codecs = new DefaultCodecs(client.getByteBufAllocator());

                Mono<IsolationLevel> isolationLevelMono = Mono.just(IsolationLevel.READ_COMMITTED);
                if (!forReplication) {
                    isolationLevelMono = getIsolationLevel(client, codecs);
                }

                return isolationLevelMono
                    .map(it -> new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, new IndefiniteStatementCache(client), it, this.configuration.isForceBinary()))
                    .delayUntil(connection -> {
                        return prepareConnection(connection, client.getByteBufAllocator(), codecs);
                    })
                    .onErrorResume(throwable -> this.closeWithError(client, throwable));
            }).onErrorMap(this::cannotConnect);
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

    private Mono<IsolationLevel> getIsolationLevel(Client client, DefaultCodecs codecs) {
        return new SimpleQueryPostgresqlStatement(client, codecs, "SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                String level = row.get(0, String.class);

                if (level == null) {
                    return IsolationLevel.READ_COMMITTED; // Best guess.
                }

                return IsolationLevel.valueOf(level.toUpperCase(Locale.US));
            })).defaultIfEmpty(IsolationLevel.READ_COMMITTED).last();
    }

    private Mono<Void> setSchema(PostgresqlConnection connection) {
        if (this.configuration.getSchema() == null) {
            return Mono.empty();
        }

        return connection.createStatement(String.format("SET SCHEMA '%s'", this.configuration.getSchema()))
            .execute()
            .then();
    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException {

        public PostgresConnectionException(String msg, @Nullable Throwable cause) {
            super(msg, cause);
        }
    }

}
