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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.message.backend.AuthenticationOk;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.StartupMessage;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#idm46428664018352">Start-up</a> message flow.
 */
public final class StartupMessageFlow {

    private StartupMessageFlow() {
    }

    /**
     * Execute the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#idm46428664018352">Start-up</a> message flow.
     *
     * @param applicationName               the name of the application connecting to the server
     * @param authenticationHandlerProvider the {@link Function} used to provide an {@link AuthenticationHandler} to use for authentication
     * @param client                        the {@link Client} to exchange messages with
     * @param database                      the database to connect to
     * @param username                      the username to authenticate with
     * @param options                       the connection options
     * @return the messages received after authentication is complete, in response to this exchange
     * @throws IllegalArgumentException if {@code applicationName}, {@code authenticationHandler}, {@code client}, or {@code username} is {@code null}
     */
    public static Flux<BackendMessage> exchange(String applicationName, Function<AuthenticationMessage, AuthenticationHandler> authenticationHandlerProvider, Client client,
                                                @Nullable String database, String username, @Nullable Map<String, String> options) {

        Assert.requireNonNull(applicationName, "applicationName must not be null");
        Assert.requireNonNull(authenticationHandlerProvider, "authenticationHandlerProvider must not be null");
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(username, "username must not be null");

        EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create();
        FluxSink<FrontendMessage> requests = requestProcessor.sink();
        AtomicReference<AuthenticationHandler> authenticationHandler = new AtomicReference<>(null);

        return client.exchange(requestProcessor.startWith(new StartupMessage(applicationName, database, username, options)))
            .handle((message, sink) -> {
                if (message instanceof AuthenticationOk) {
                    requests.complete();
                } else if (message instanceof AuthenticationMessage) {
                    try {
                        AuthenticationMessage authenticationMessage = (AuthenticationMessage) message;

                        if (authenticationHandler.get() == null) {
                            authenticationHandler.compareAndSet(null, authenticationHandlerProvider.apply(authenticationMessage));
                        }

                        FrontendMessage response = authenticationHandler.get().handle(authenticationMessage);
                        if (response != null) {
                            requests.next(response);
                        }
                    } catch (Exception e) {
                        requests.error(e);
                        sink.error(e);
                    }
                } else {
                    sink.next(message);
                }
            });
    }

}
