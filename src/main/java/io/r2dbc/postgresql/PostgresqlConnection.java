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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.client.TransactionStatus.OPEN;

/**
 * An implementation of {@link Connection} for connecting to a PostgreSQL database.
 */
public final class PostgresqlConnection implements Connection {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Client client;

    private final Codecs codecs;

    private final boolean forceBinary;

    private final PortalNameSupplier portalNameSupplier;

    private final StatementCache statementCache;

    private final Flux<Integer> validationQuery;

    private volatile boolean autoCommit;

    private volatile IsolationLevel isolationLevel;

    PostgresqlConnection(Client client, Codecs codecs, PortalNameSupplier portalNameSupplier, StatementCache statementCache, boolean forceBinary) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.portalNameSupplier = Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        this.statementCache = Assert.requireNonNull(statementCache, "statementCache must not be null");
        this.forceBinary = forceBinary;
        this.validationQuery = new SimpleQueryPostgresqlStatement(this.client, this.codecs, "SELECT 1").fetchSize(0).execute().flatMap(PostgresqlResult::getRowsUpdated);
        this.autoCommit = true;
        this.isolationLevel = IsolationLevel.READ_COMMITTED;
    }

    @Override
    public Mono<Void> beginTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (IDLE == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, "BEGIN")
                    .handle(PostgresqlExceptionFactory::handleErrorResponse);
            } else {
                this.logger.debug("Skipping begin transaction because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> close() {
        return this.client.close()
            .then(Mono.empty());
    }

    @Override
    public Mono<Void> commitTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, "COMMIT")
                    .handle(PostgresqlExceptionFactory::handleErrorResponse);
            } else {
                this.logger.debug("Skipping commit transaction because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public PostgresqlBatch createBatch() {
        return new PostgresqlBatch(this.client, this.codecs);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return beginTransaction()
            .then(useTransactionStatus(transactionStatus -> {
                if (OPEN == transactionStatus) {

                    if (this.autoCommit) {
                        logger.debug("Setting auto-commit mode to [false]");
                    }
                    return SimpleQueryMessageFlow.exchange(this.client, String.format("SAVEPOINT %s", name))
                        .doOnComplete(() -> this.autoCommit = false)
                        .handle(PostgresqlExceptionFactory::handleErrorResponse);
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
            return new SimpleQueryPostgresqlStatement(this.client, this.codecs, sql);
        } else if (ExtendedQueryPostgresqlStatement.supports(sql)) {
            return new ExtendedQueryPostgresqlStatement(this.client, this.codecs, this.portalNameSupplier, sql, this.statementCache, this.forceBinary);
        } else {
            throw new IllegalArgumentException(String.format("Statement '%s' cannot be created. This is often due to the presence of both multiple statements and parameters at the same time.", sql));
        }
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {
        return this.autoCommit;
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, String.format("RELEASE SAVEPOINT %s", name))
                    .handle(PostgresqlExceptionFactory::handleErrorResponse);
            } else {
                this.logger.debug("Skipping release savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, "ROLLBACK")
                    .handle(PostgresqlExceptionFactory::handleErrorResponse);
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
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, String.format("ROLLBACK TO SAVEPOINT %s", name))
                    .handle(PostgresqlExceptionFactory::handleErrorResponse);
            } else {
                this.logger.debug("Skipping rollback transaction to savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {

        return Mono.defer(() -> {

            logger.debug(String.format("Setting auto-commit mode to [%s]", autoCommit));

            if (this.autoCommit != autoCommit) {

                logger.debug("Committing pending transactions");
                return commitTransaction().doOnSuccess(ignore -> this.autoCommit = autoCommit);
            }

            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return withTransactionStatus(getTransactionIsolationLevelQuery(isolationLevel))
            .flatMapMany(query -> SimpleQueryMessageFlow.exchange(this.client, query))
            .handle(PostgresqlExceptionFactory::handleErrorResponse)
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
                    logger.debug("Validation failed", t);
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
            .then();
    }

    private <T> Mono<T> withTransactionStatus(Function<TransactionStatus, T> f) {
        return Mono.defer(() -> Mono.just(f.apply(this.client.getTransactionStatus())));
    }

}
