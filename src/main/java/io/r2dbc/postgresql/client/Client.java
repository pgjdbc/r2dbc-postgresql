/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    /**
     * Release any resources held by the {@link Client}.
     *
     * @return a {@link Mono} that indicates that a client has been closed
     */
    Mono<Void> close();

    /**
     * Perform an exchange of messages.
     *
     * @param requests the publisher of outbound messages
     * @return a {@link Flux} of incoming messages that ends with the end of the frame (i.e. reception of a {@link ReadyForQuery} message.
     * @throws IllegalArgumentException if {@code requests} is {@code null}
     */
    Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests);

    /**
     * Returns the {@link ByteBufAllocator}.
     *
     * @return the {@link ByteBufAllocator}
     */
    ByteBufAllocator getByteBufAllocator();

    /**
     * Returns the connected process id if it has been communicated.
     *
     * @return the connected process id if it has been communicated
     */
    Optional<Integer> getProcessId();

    /**
     * Returns the connected process secret key if it has been communicated.
     *
     * @return the connected process secret key if it has been communicated
     */
    Optional<Integer> getSecretKey();

    /**
     * Returns the current transaction status.
     *
     * @return the current transaction status
     */
    TransactionStatus getTransactionStatus();

    /**
     * Add a consumer of notification messages.
     *
     * @param consumer the consumer of notification messages
     * @return a new {@link Disposable} that can be used to cancel the underlying subscription.
     * @throws IllegalArgumentException if {@code consumer} is {@code null}
     */
    Disposable addNotificationListener(Consumer<NotificationResponse> consumer);
}
