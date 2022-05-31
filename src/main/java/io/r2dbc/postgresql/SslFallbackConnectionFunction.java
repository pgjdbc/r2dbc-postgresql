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

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link ConnectionFunction} providing SSL-fallback options depending on the configured {@link SSLMode}.
 *
 * @since 1.0
 */
final class SslFallbackConnectionFunction implements ConnectionFunction {

    private final SSLConfig sslConfig;

    private final ConnectionFunction upstreamFunction;

    SslFallbackConnectionFunction(SSLConfig sslConfig, ConnectionFunction upstreamFunction) {
        this.sslConfig = sslConfig;
        this.upstreamFunction = upstreamFunction;
    }

    @Override
    public Mono<Client> connect(SocketAddress endpoint, ConnectionSettings settings) {

        Mono<Client> connect = this.upstreamFunction.connect(endpoint, settings);
        SSLMode sslMode = this.sslConfig.getSslMode();

        if (sslMode == SSLMode.ALLOW || sslMode == SSLMode.PREFER) {

            Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;

            connect = connect.onErrorResume(isAuthSpecificationError.and(e -> sslMode == SSLMode.ALLOW), fallback(SSLMode.REQUIRE, endpoint, settings))
                .onErrorResume(isAuthSpecificationError.and(e -> sslMode == SSLMode.PREFER), fallback(SSLMode.DISABLE, endpoint, settings));
        }

        return connect;
    }

    private Function<Throwable, Mono<Client>> fallback(SSLMode sslMode, SocketAddress endpoint, ConnectionSettings settings) {

        return e -> {

            ConnectionSettings settingsToUse = settings.mutate(builder -> builder.sslConfig(this.sslConfig.mutateMode(sslMode)));

            return this.upstreamFunction.connect(endpoint, settingsToUse)
                .onErrorResume(sslAuthError -> {
                    e.addSuppressed(sslAuthError);
                    return Mono.error(e);
                });
        };
    }

}
