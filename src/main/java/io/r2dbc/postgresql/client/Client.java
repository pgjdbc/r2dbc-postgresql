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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    /**
     * Add a consumer of notification messages. Notifications received by this connection are sent to the {@link Consumer notification consumer}. Note that connection errors and events such as
     * disconnects are not visible to the {@link Consumer notification consumer}.
     *
     * @param consumer the consumer of notification messages
     * @return a new {@link Disposable} that can be used to cancel the underlying subscription.
     * @throws IllegalArgumentException if {@code consumer} is {@code null}
     */
    Disposable addNotificationListener(Consumer<NotificationResponse> consumer);

    /**
     * Add a consumer of notification messages. Notifications received by this connection are sent to the {@link Subscriber notification listener}. When the client gets {@link #close() closed}, the
     * subscription {@link Subscriber#onComplete() completes normally}. Otherwise (transport connection disconnected unintentionally) with an {@link R2dbcNonTransientResourceException error}.
     *
     * @param consumer the consumer of notification messages
     * @return a new {@link Disposable} that can be used to cancel the underlying subscription.
     * @throws IllegalArgumentException if {@code consumer} is {@code null}
     * @since 0.8.1
     */
    Disposable addNotificationListener(Subscriber<NotificationResponse> consumer);

    /**
     * Release any resources held by the {@link Client}.
     *
     * @return a {@link Mono} that indicates that a client has been closed
     */
    Mono<Void> close();

    /**
     * Perform an exchange of messages. Note that the {@link ReadyForQuery} frame is not emitted through the resulting {@link Flux}.
     *
     * @param requests the publisher of outbound messages
     * @return a {@link Flux} of incoming messages that ends with the end of conversation (i.e. reception of a {@link ReadyForQuery} message. Th
     * @throws IllegalArgumentException if {@code requests} is {@code null}
     */
    default Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests) {
        return this.exchange(it -> it.getClass() == ReadyForQuery.class, requests);
    }

    /**
     * Perform an exchange of messages.
     *
     * @param takeUntil the predicate that signals the resulting {@link Flux} to terminate. Typically a check if the {@link BackendMessage} is the last frame of a conversation. Note that the
     *                  {@link BackendMessage} that matches the predicate is not emitted through the resulting {@link Flux}.
     * @param requests  the publisher of outbound messages
     * @return a {@link Flux} of incoming messages that ends with the end of conversation matching {@code takeUntil}. (i.e. reception of a {@link ReadyForQuery} message.
     * @throws IllegalArgumentException if {@code requests} is {@code null}
     * @since 0.9
     */
    Flux<BackendMessage> exchange(Predicate<BackendMessage> takeUntil, Publisher<FrontendMessage> requests);

    /**
     * Send one message without waiting for response.
     *
     * @param message outbound message
     * @throws IllegalArgumentException if {@code message} is {@code null}
     * @since 0.9
     */
    void send(FrontendMessage message);

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
     * Return the server version.
     *
     * @return the server version from {@code server_version}/{@code server_version_num} startup parameters.
     */
    Version getVersion();

    /**
     * Returns whether the client is connected to a server.
     *
     * @return {@literal true} if the client is connected to a server.
     */
    boolean isConnected();

}
