/*
 * Copyright 2020 the original author or authors.
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

import io.r2dbc.postgresql.message.frontend.CancelRequest;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.9">Cancel Request</a> message flow.
 */
public final class CancelRequestMessageFlow {

    private CancelRequestMessageFlow() {
    }

    /**
     * Execute the <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.9">Cancel Request</a> message flow.
     *
     * @param client the {@link Client} to exchange messages with
     * @return the messages received after authentication is complete, in response to this exchange
     * @throws IllegalArgumentException if {@code Client} is {@code null}
     */
    public static Mono<Void> exchange(Client client, int processId, int secretKey) {
        Assert.requireNonNull(client, "client must not be null");

        return client.exchange(Mono.just(new CancelRequest(processId, secretKey))).then();
    }

}
