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

/**
 * Interface defining a function how to connect to a single {@link SocketAddress endpoint} applying {@link ConnectionSettings}.
 * <p>A connection function is a low-level utility whose result is a valid {@link Client} object. Connection functions may perform multiple connection attempts (e.g. SSL handshake downgrading).
 * Topology discovery is a higher-level concept that is typically encapsulated as part of a {@link ConnectionStrategy}.
 *
 * @see ConnectionStrategy
 * @since 1.0
 */
@FunctionalInterface
public interface ConnectionFunction {

    /**
     * Establish a connection to the given {@link SocketAddress endpoint} applying {@link ConnectionSettings}.
     *
     * @param endpoint the endpoint to connect to
     * @param settings the settings to apply
     * @return a mono that connects to the given endpoint upon subscription
     * @throws IllegalArgumentException if {@code socketAddress} or {@code settings} is {@code null}
     */
    Mono<Client> connect(SocketAddress endpoint, ConnectionSettings settings);

}
