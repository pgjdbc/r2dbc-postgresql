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

import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.core.publisher.Mono;

import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a PostgreSQL database.
 */
public final class PostgresqlConnectionFactory implements ConnectionFactory {

    private final Function<SSLConfig, Mono<? extends Client>> clientFactory;

    private final PostgresqlConnectionConfiguration configuration;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use connections
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public PostgresqlConnectionFactory(PostgresqlConnectionConfiguration configuration) {
        this.clientFactory = sslConfig -> ReactorNettyClient.connect(configuration.getHost(), configuration.getPort(), configuration.getConnectTimeout(), sslConfig).cast(Client.class);
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
    }

    PostgresqlConnectionFactory(Function<SSLConfig, Mono<? extends Client>> clientFactory, PostgresqlConnectionConfiguration configuration) {
        this.clientFactory = Assert.requireNonNull(clientFactory, "clientFactory must not be null");
        this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
    }

    @Override
    public Mono<PostgresqlConnection> create() {
        SSLConfig sslConfig = this.configuration.getSslConfig();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.tryConnectWithConfig(sslConfig)
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW),
                e -> this.tryConnectWithConfig(sslConfig.mutateMode(SSLMode.REQUIRE))
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER),
                e -> this.tryConnectWithConfig(sslConfig.mutateMode(SSLMode.DISABLE))
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .flatMap(client -> {
                DefaultCodecs codecs = new DefaultCodecs(client.getByteBufAllocator());

                return this.getIsolationLevel(client, codecs)
                    .map(it -> new PostgresqlConnection(client, codecs, DefaultPortalNameSupplier.INSTANCE, new IndefiniteStatementCache(client), it, configuration.isForceBinary()))
                    .delayUntil(this::setSchema)
                    .onErrorResume(throwable -> this.closeWithError(client, throwable));
            });
    }

    private Mono<Client> tryConnectWithConfig(SSLConfig sslConfig) {
        return this.clientFactory.apply(sslConfig)
            .delayUntil(client -> StartupMessageFlow
                .exchange(this.configuration.getApplicationName(), this::getAuthenticationHandler, client, this.configuration.getDatabase(), this.configuration.getUsername(),
                    this.configuration.getOptions())
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse))
            .cast(Client.class);
    }

    private Mono<PostgresqlConnection> closeWithError(Client client, Throwable throwable) {
        return client.close()
            .then(Mono.error(new R2dbcNonTransientResourceException(
                String.format("Cannot connect to %s:%d", this.configuration.getHost(), this.configuration.getPort()), throwable
            )));
    }

    @Override
    public PostgresqlConnectionFactoryMetadata getMetadata() {
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

}
