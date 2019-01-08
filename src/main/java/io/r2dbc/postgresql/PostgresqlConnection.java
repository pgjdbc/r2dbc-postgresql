/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final PortalNameSupplier portalNameSupplier;

    private final StatementCache statementCache;

    PostgresqlConnection(Client client, Codecs codecs, PortalNameSupplier portalNameSupplier, StatementCache statementCache) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.portalNameSupplier = Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        this.statementCache = Assert.requireNonNull(statementCache, "statementCache must not be null");
    }

    @Override
    public Mono<Void> beginTransaction() {
        return useTransactionStatus(transactionStatus -> {
            if (IDLE == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, "BEGIN")
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
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
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
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

        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, String.format("SAVEPOINT %s", name))
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
            } else {
                this.logger.debug("Skipping create savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public PostgresqlStatement<?> createStatement(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        if (SimpleQueryPostgresqlStatement.supports(sql)) {
            return new SimpleQueryPostgresqlStatement(this.client, this.codecs, sql);
        } else if (ExtendedQueryPostgresqlStatement.supports(sql)) {
            return new ExtendedQueryPostgresqlStatement(this.client, this.codecs, this.portalNameSupplier, sql, this.statementCache);
        } else {
            throw new IllegalArgumentException(String.format("Statement '%s' cannot be created. This is often due to the presence of both multiple statements and parameters at the same time.", sql));
        }
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        Assert.requireNonNull(name, "name must not be null");

        return useTransactionStatus(transactionStatus -> {
            if (OPEN == transactionStatus) {
                return SimpleQueryMessageFlow.exchange(this.client, String.format("RELEASE SAVEPOINT %s", name))
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
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
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
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
                    .handle(PostgresqlServerErrorException::handleErrorResponse);
            } else {
                this.logger.debug("Skipping rollback transaction to savepoint because status is {}", transactionStatus);
                return Mono.empty();
            }
        });
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return withTransactionStatus(getTransactionIsolationLevelQuery(isolationLevel))
            .flatMapMany(query -> SimpleQueryMessageFlow.exchange(this.client, query))
            .handle(PostgresqlServerErrorException::handleErrorResponse)
            .then();
    }

    @Override
    public String toString() {
        return "PostgresqlConnection{" +
            "client=" + this.client +
            ", codecs=" + this.codecs +
            ", portalNameSupplier=" + this.portalNameSupplier +
            ", statementCache=" + this.statementCache +
            '}';
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
