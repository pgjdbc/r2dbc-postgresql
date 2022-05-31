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
import reactor.core.publisher.Mono;

/**
 * Interface defining a connection strategy on how to obtain a Postgres {@link Client} object.
 * <p>
 * Typically, connection strategies use a {@link ConnectionFunction} and are configured with a connection endpoint to establish a client connection to the target server as the {@link #connect()}
 * method does not take any parameters.
 *
 * @see ConnectionFunction
 * @since 1.0
 */
@FunctionalInterface
public interface ConnectionStrategy {

    /**
     * Establish a connection to a target server that is determined by this connection strategy.
     *
     * @return a mono that initiates the connection upon subscription.
     */
    Mono<Client> connect();

}
