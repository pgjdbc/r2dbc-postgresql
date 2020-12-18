/*
 * Copyright 2017 the original author or authors.
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

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.GeneratedValuesUtils;
import io.r2dbc.postgresql.util.Operators;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static io.r2dbc.postgresql.util.PredicateUtils.or;

/**
 * {@link Statement} using the <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4">Simple Query Flow</a>. Can use the {@link ExtendedQueryPostgresqlStatement extended
 * flow} when a {@link #fetchSize(int) fetch size} is specified.
 */
final class SimpleQueryPostgresqlStatement implements PostgresqlStatement {

    private static final Predicate<BackendMessage> WINDOW_UNTIL = or(CommandComplete.class::isInstance, EmptyQueryResponse.class::isInstance, ErrorResponse.class::isInstance);

    private final ConnectionResources resources;

    private final String sql;

    private String[] generatedColumns;

    private int fetchSize;

    SimpleQueryPostgresqlStatement(ConnectionResources resources, String sql) {
        this.resources = Assert.requireNonNull(resources, "resources must not be null");
        this.sql = Assert.requireNonNull(sql, "sql must not be null");
        fetchSize(isBatch() ? NO_LIMIT : this.resources.getConfiguration().getFetchSize(sql));
    }

    private boolean isBatch() {
        return this.sql.contains(";");
    }

    @Override
    public SimpleQueryPostgresqlStatement add() {
        return this;
    }

    @Override
    public SimpleQueryPostgresqlStatement bind(@Nullable String identifier, @Nullable Object value) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bind(int index, @Nullable Object value) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bindNull(@Nullable String identifier, @Nullable Class<?> type) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bindNull(int index, @Nullable Class<?> type) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public Flux<io.r2dbc.postgresql.api.PostgresqlResult> execute() {
        if (this.generatedColumns == null) {
            return execute(this.sql);
        }

        return execute(GeneratedValuesUtils.augment(this.sql, this.generatedColumns));
    }

    @Override
    public SimpleQueryPostgresqlStatement fetchSize(int rows) {
        Assert.isTrue(rows >= 0, "Fetch size must be greater or equal zero");

        if (rows != NO_LIMIT) {
            Assert.isTrue(!isBatch(), "Fetch size can only be used with a single SQL statement");
        }

        this.fetchSize = rows;

        return this;
    }

    @Override
    public SimpleQueryPostgresqlStatement returnGeneratedValues(String... columns) {
        Assert.requireNonNull(columns, "columns must not be null");

        if (GeneratedValuesUtils.hasReturningClause(this.sql)) {
            throw new IllegalStateException("Statement already includes RETURNING clause");
        }

        if (!GeneratedValuesUtils.isSupportedCommand(this.sql)) {
            throw new IllegalStateException("Statement is not a DELETE, INSERT, or UPDATE command");
        }

        this.generatedColumns = columns;
        return this;
    }

    @Override
    public String toString() {
        return "SimpleQueryPostgresqlStatement{" +
            "context=" + this.resources +
            ", sql='" + this.sql + '\'' +
            '}';
    }

    static boolean supports(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        return sql.trim().isEmpty() || !sql.contains("$1");
    }

    private Flux<io.r2dbc.postgresql.api.PostgresqlResult> execute(String sql) {

        ExceptionFactory factory = ExceptionFactory.withSql(sql);

        if (this.fetchSize != NO_LIMIT) {

            Flux<BackendMessage> messages = ExtendedFlowDelegate.runQuery(this.resources, factory, sql, Binding.EMPTY, Collections.emptyList(), this.fetchSize);

            return Flux.just(new PostgresqlResult(this.resources, messages, factory));
        }

        return SimpleQueryMessageFlow
            .exchange(this.resources.getClient(), sql)
            .windowUntil(WINDOW_UNTIL)
            .map(dataRow -> PostgresqlResult.toResult(this.resources, dataRow, factory))
            .cast(io.r2dbc.postgresql.api.PostgresqlResult.class)
            .as(Operators::discardOnCancel)
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release);
    }

}
