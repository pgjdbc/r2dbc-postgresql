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
import reactor.core.publisher.Mono;

import java.net.SocketAddress;

final class SingleHostConnectionStrategy implements ConnectionStrategy {

    private final ConnectionFunction connectionFunction;

    private final SocketAddress endpoint;

    private final ConnectionSettings connectionSettings;

    public SingleHostConnectionStrategy(ConnectionFunction connectionFunction, SocketAddress endpoint, ConnectionSettings connectionSettings) {
        this.connectionFunction = connectionFunction;
        this.endpoint = endpoint;
        this.connectionSettings = connectionSettings;
    }

    @Override
    public Mono<Client> connect() {
        return this.connectionFunction.connect(this.endpoint, this.connectionSettings);
    }

    @Override
    public String toString() {
        return this.endpoint.toString();
    }

}
