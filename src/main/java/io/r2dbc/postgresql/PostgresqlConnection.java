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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.Operators;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.client.TransactionStatus.OPEN;

/**
 * An implementation of {@link Connection} for connecting to a PostgreSQL database.
 */
final class PostgresqlConnection implements io.r2dbc.postgresql.api.PostgresqlConnection {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final ConnectionContext context;

    private final Client client;

    private final Codecs codecs;

    private final boolean forceBinary;

    private final PortalNameSupplier portalNameSupplier;

    private final StatementCache statementCache;

    private final Flux<Integer> validationQuery;

    private final AtomicReference<NotificationAdapter> notificationAdapter = new AtomicReference<>();

    private volatile IsolationLevel isolationLevel;

    PostgresqlConnection(Client client, Codecs codecs, PortalNameSupplier portalNameSupplier, StatementCache statementCache, IsolationLevel isolationLevel, boolean forceBinary) {
        this.context = new ConnectionContext(client, codecs, this);
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.portalNameSupplier = Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        this.statementCache = Assert.requireNonNull(statementCache, "statementCache must not be null");
        this.forceBinary = forceBinary;
        this.isolationLevel = Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
        this.validationQuery = new SimpleQueryPostgresqlStatement(this.context, "SELECT 1").fetchSize(0).execute().flatMap(PostgresqlResult::getRowsUpdated);
    }

    Client getClient() {
        return this.client;
    }

    @Override
    public Mono<Void> beginTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (IDLE == transactionStatus) {
                return exchange("BEGIN");
            } else {
                this.logger.debug("Skipping begin transaction because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> close() {
        return this.client.close().doOnSubscribe(subscription -> {

            NotificationAdapter notificationAdapter = this.notificationAdapter.get();

            if (notificationAdapter != null && this.notificationAdapter.compareAndSet(notificationAdapter, null)) {
                notificationAdapter.dispose();
            }
        }).then(Mono.empty());
    }

    @Override
    public Mono<Void> cancelRunningQuery() {
        return this.client.cancelRunningQuery();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return exchange("COMMIT");
            } else {
                this.logger.debug("Skipping commit transaction because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public PostgresqlBatch createBatch() {
        return new PostgresqlBatch(this.context);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return beginTransaction()
            .then(useTransactionStatus(transactionStatus -> {
                if (OPEN == transactionStatus) {
                    return exchange(String.format("SAVEPOINT %s", name));
                } else {
                    this.logger.debug("Skipping create savepoint because status is {}", transactionStatus);
                    return Mono.empty();
                }
            }));
    }

    @Override
    public PostgresqlStatement createStatement(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        if (SimpleQueryPostgresqlStatement.supports(sql)) {
            return new SimpleQueryPostgresqlStatement(this.context, sql);
        } else if (ExtendedQueryPostgresqlStatement.supports(sql)) {
            return new ExtendedQueryPostgresqlStatement(this.context, this.portalNameSupplier, sql, this.statementCache, this.forceBinary);
        } else {
            throw new IllegalArgumentException(String.format("Statement '%s' cannot be created. This is often due to the presence of both multiple statements and parameters at the same time.", sql));
        }
    }

    /**
     * Return a {@link Flux} of {@link Notification} received from {@code LISTEN} registrations.
     * The stream is a hot stream producing messages as they are received.
     *
     * @return a hot {@link Flux} of {@link Notification Notifications}.
     */
    @Override
    public Flux<Notification> getNotifications() {

        NotificationAdapter notifications = this.notificationAdapter.get();

        if (notifications == null) {

            notifications = new NotificationAdapter();

            if (this.notificationAdapter.compareAndSet(null, notifications)) {
                notifications.register(this.client);
            } else {
                notifications = this.notificationAdapter.get();
            }
        }

        return notifications.getEvents();
    }

    @Override
    public PostgresqlConnectionMetadata getMetadata() {
        return new PostgresqlConnectionMetadata(this.client.getVersion());
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {

        if (this.client.getTransactionStatus() == IDLE) {
            return true;
        }

        return false;
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return exchange(String.format("RELEASE SAVEPOINT %s", name));
            } else {
                this.logger.debug("Skipping release savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (IDLE != transactionStatus) {
                return exchange("ROLLBACK");
            } else {
                this.logger.debug("Skipping rollback transaction because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (IDLE != transactionStatus) {
                return exchange(String.format("ROLLBACK TO SAVEPOINT %s", name));
            } else {
                this.logger.debug("Skipping rollback transaction to savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {

        return useTransactionStatus(transactionStatus -> {

            this.logger.debug(String.format("Setting auto-commit mode to [%s]", autoCommit));

            if (isAutoCommit()) {
                if (!autoCommit) {
                    this.logger.debug("Beginning transaction");
                    return beginTransaction();
                }
            } else {

                if (autoCommit) {
                    this.logger.debug("Committing pending transactions");
                    return commitTransaction();
                }
            }

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return withTransactionStatus(getTransactionIsolationLevelQuery(isolationLevel))
            .flatMapMany(this::exchange)
            .then()
            .doOnSuccess(ignore -> this.isolationLevel = isolationLevel);
    }

    @Override
    public String toString() {
        return "PostgresqlConnection{" +
            "client=" + this.client +
            ", codecs=" + this.codecs +
            ", forceBinary=" + this.forceBinary +
            ", portalNameSupplier=" + this.portalNameSupplier +
            ", statementCache=" + this.statementCache +
            '}';
    }

    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(this.client::isConnected);
        }

        return Mono.create(sink -> {

            if (!this.client.isConnected()) {
                sink.success(false);
                return;
            }

            this.validationQuery.subscribe(new CoreSubscriber<Integer>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Integer.MAX_VALUE);
                }

                @Override
                public void onNext(Integer integer) {

                }

                @Override
                public void onError(Throwable t) {
                    PostgresqlConnection.this.logger.debug("Validation failed", t);
                    sink.success(false);
                }

                @Override
                public void onComplete() {
                    sink.success(true);
                }
            });
        });
    }

    private static Function<TransactionStatus, String> getTransactionIsolationLevelQuery(IsolationLevel isolationLevel) {
        return transactionStatus -> {
            if (transactionStatus == OPEN) {
                return String.format("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
            } else {
                return String.format("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql());
            }
        };
    }

    private Mono<Void> useTransactionStatus(Function<TransactionStatus, Publisher<?>> f) {
        return Flux.defer(() -> f.apply(this.client.getTransactionStatus()))
            .as(Operators::discardOnCancel)
            .then();
    }

    private <T> Mono<T> withTransactionStatus(Function<TransactionStatus, T> f) {
        return Mono.defer(() -> Mono.just(f.apply(this.client.getTransactionStatus())));
    }

    private Publisher<?> exchange(String sql) {
        ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);
        return SimpleQueryMessageFlow.exchange(this.client, sql)
            .handle(exceptionFactory::handleErrorResponse)
            .as(Operators::discardOnCancel);
    }

    /**
     * Adapter to publish {@link Notification}s.
     */
    static class NotificationAdapter {

        private final DirectProcessor<Notification> processor = DirectProcessor.create();

        private final FluxSink<Notification> sink = this.processor.sink();

        @Nullable
        private volatile Disposable subscription = null;

        void dispose() {
            Disposable subscription = this.subscription;
            if (subscription != null && !subscription.isDisposed()) {
                subscription.dispose();
            }
        }

        void register(Client client) {

            this.subscription = client.addNotificationListener(new Subscriber<NotificationResponse>() {

                @Override
                public void onSubscribe(Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(NotificationResponse notificationResponse) {
                    NotificationAdapter.this.sink.next(new NotificationResponseWrapper(notificationResponse));
                }

                @Override
                public void onError(Throwable throwable) {
                    NotificationAdapter.this.sink.error(throwable);
                }

                @Override
                public void onComplete() {
                    NotificationAdapter.this.sink.complete();
                }
            });
        }

        Flux<Notification> getEvents() {
            return this.processor;
        }
    }

}
