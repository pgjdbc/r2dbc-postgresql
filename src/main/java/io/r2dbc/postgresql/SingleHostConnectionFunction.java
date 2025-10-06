/*
 * Copyright 2022 the original author or authors.
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
import io.r2dbc.postgresql.authentication.UsernameAndPassword;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionContext;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.PostgresStartupParameterProvider;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;

final class SingleHostConnectionFunction implements ConnectionFunction {

    private final ConnectionFunction upstreamFunction;

    private final PostgresqlConnectionConfiguration configuration;

    SingleHostConnectionFunction(ConnectionFunction upstreamFunction, PostgresqlConnectionConfiguration configuration) {
        this.upstreamFunction = upstreamFunction;
        this.configuration = configuration;
    }

    @Override
    public Mono<Client> connect(SocketAddress endpoint, ConnectionSettings settings) {

        return this.upstreamFunction.connect(endpoint, settings)
            .delayUntil(client -> getCredentials().flatMapMany(credentials -> StartupMessageFlow
                    .exchange(auth -> getAuthenticationHandler(auth, credentials, client.getContext()), client, this.configuration.getDatabase(), credentials.getUsername(),
                        getParameterProvider(this.configuration, settings)))
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse));
    }

    private static PostgresStartupParameterProvider getParameterProvider(PostgresqlConnectionConfiguration configuration, ConnectionSettings settings) {
        return new PostgresStartupParameterProvider(configuration.getApplicationName(), configuration.getTimeZone(), settings);
    }

    protected AuthenticationHandler getAuthenticationHandler(AuthenticationMessage message, UsernameAndPassword usernameAndPassword, ConnectionContext context) {
        return AuthenticationHandler.getAuthenticationHandler(message, usernameAndPassword, context);
    }

    Mono<UsernameAndPassword> getCredentials() {

        return Mono.zip(Mono.from(this.configuration.getUsername()).single(), Mono.from(this.configuration.getPassword()).singleOptional()).map(it -> {
            return new UsernameAndPassword(it.getT1(), it.getT2().orElse(null));
        });
    }

}
