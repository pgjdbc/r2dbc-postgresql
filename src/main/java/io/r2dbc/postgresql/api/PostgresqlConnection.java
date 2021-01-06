/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.api;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link Connection} for connecting to a PostgreSQL database.
 */
public interface PostgresqlConnection extends Connection {

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> beginTransaction();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> close();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> commitTransaction();

    /**
     * {@inheritDoc}
     */
    @Override
    PostgresqlBatch createBatch();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> createSavepoint(String name);

    /**
     * {@inheritDoc}
     */
    @Override
    PostgresqlStatement createStatement(String sql);

    /**
     * Return a {@link Flux} of {@link Notification} received from {@code LISTEN} registrations. The stream is a hot stream producing messages as they are received. Notifications received by this
     * connection are published as they are received. When the client gets {@link #close() closed}, the subscription {@link Subscriber#onComplete() completes normally}. Otherwise (transport
     * connection disconnected unintentionally) with an {@link R2dbcNonTransientResourceException error}.
     *
     * @return a hot {@link Flux} of {@link Notification Notifications}
     */
    Flux<Notification> getNotifications();

    /**
     * {@inheritDoc}
     */
    @Override
    PostgresqlConnectionMetadata getMetadata();

    /**
     * {@inheritDoc}
     */
    @Override
    IsolationLevel getTransactionIsolationLevel();

    /**
     * {@inheritDoc}
     */
    @Override
    boolean isAutoCommit();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> releaseSavepoint(String name);

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> rollbackTransaction();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> rollbackTransactionToSavepoint(String name);

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> setAutoCommit(boolean autoCommit);

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel);

    /**
     * {@inheritDoc}
     */
    @Override
    String toString();

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Boolean> validate(ValidationDepth depth);

}
