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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.frontend.CancelRequest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

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
     *
     * @see PostgresTransactionDefinition
     */
    @Override
    Mono<Void> beginTransaction(TransactionDefinition definition);

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
     * Obtain a {@link CopyInBuilder} to configure a {@code COPY FROM STDIN} operation for very fast copying into a database table.
     *
     * @param sql the COPY … FROM STDIN sql statement
     * @return the builder to configure the copy operation.
     * @since 1.0
     */
    CopyInBuilder copyIn(String sql);

    /**
     * Use {@code COPY FROM STDIN} for very fast copying into a database table.
     *
     * @param sql   the COPY … FROM STDIN sql statement
     * @param stdin the ByteBuf publisher
     * @return a {@link Mono} with the amount of rows inserted
     * @see CopyInBuilder
     * @since 1.0
     */
    default Mono<Long> copyIn(String sql, Publisher<ByteBuf> stdin) {
        return copyIn(sql).from(stdin).build();
    }

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
     * Cancel currently running query by sending {@link CancelRequest} to a server.
     *
     * @return a {@link Mono} that indicates that a cancel frame was delivered to the backend
     * @since 0.9
     */
    Mono<Void> cancelRequest();

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
     * Sets Lock Acquisition Timeout through {@code SET LOCK_TIMEOUT}.
     *
     * @return a {@link Mono} that indicates that a lockTimeout frame was delivered to the backend
     * @since 0.8.9
     */
    Mono<Void> setLockWaitTimeout(Duration lockTimeout);

    /**
     * Sets Statement Timeout through {@code SET STATEMENT_TIMEOUT}.
     *
     * @return a {@link Mono} that indicates that a statementTimeout frame was delivered to the backend
     * @since 0.8.9
     */
    Mono<Void> setStatementTimeout(Duration statementTimeout);

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
