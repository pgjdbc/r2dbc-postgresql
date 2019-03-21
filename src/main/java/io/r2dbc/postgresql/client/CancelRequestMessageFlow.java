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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.frontend.CancelRequest;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#idm46428663888448">Cancel Request</a> message flow.
 */
public final class CancelRequestMessageFlow {

    private CancelRequestMessageFlow() {
    }

    /**
     * Execute the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#idm46428663888448">Cancel Request</a> message flow.
     *
     * @param client the {@link Client} to exchange messages with
     * @return the messages received after authentication is complete, in response to this exchange
     * @throws IllegalArgumentException if {@code Client} is {@code null}
     */
    public static Flux<BackendMessage> exchange(Client client) {
        Assert.requireNonNull(client, "client must not be null");

        int processId = client.getProcessId().orElseThrow(() -> new IllegalStateException("Connection does not yet have a processId"));
        int secretKey = client.getSecretKey().orElseThrow(() -> new IllegalStateException("Connection does not yet have a secretKey"));

        return client.exchange(Mono.just(new CancelRequest(processId, secretKey)));
    }

}
