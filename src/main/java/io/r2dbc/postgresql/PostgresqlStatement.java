/*
 * Copyright 2021 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.ConnectionContext;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.GeneratedValuesUtils;
import io.r2dbc.postgresql.util.Operators;
import io.r2dbc.postgresql.util.sql.BasicPostgresqlSqlLexer;
import io.r2dbc.postgresql.util.sql.TokenizedSql;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static io.r2dbc.postgresql.util.PredicateUtils.or;

/**
 * {@link Statement}.
 */
final class PostgresqlStatement implements io.r2dbc.postgresql.api.PostgresqlStatement {

    private static final Predicate<BackendMessage> WINDOW_UNTIL = or(CommandComplete.class::isInstance, EmptyQueryResponse.class::isInstance, ErrorResponse.class::isInstance);

    private final ArrayDeque<Binding> bindings;

    private final ConnectionResources resources;

    private final ConnectionContext connectionContext;

    private final TokenizedSql tokenizedSql;

    private int fetchSize;

    private String[] generatedColumns;

    PostgresqlStatement(ConnectionResources resources, String sql) {
        this.resources = Assert.requireNonNull(resources, "resources must not be null");
        this.tokenizedSql = BasicPostgresqlSqlLexer.tokenize(Assert.requireNonNull(sql, "sql must not be null"));
        this.connectionContext = resources.getClient().getContext();
        this.bindings = new ArrayDeque<>(tokenizedSql.getParameterCount());

        if (tokenizedSql.getStatementCount() > 1 && tokenizedSql.getParameterCount() > 0) {
            throw new IllegalArgumentException(String.format("Statement '%s' cannot be created. This is often due to the presence of both multiple statements and parameters at the same time.", sql));
        }

        fetchSize(this.resources.getConfiguration().getFetchSize(sql));
    }

    @Override
    public PostgresqlStatement add() {
        Binding binding = bindings.peekLast();
        if (binding != null) {
            binding.validate();
        }
        this.bindings.add(new Binding(tokenizedSql.getParameterCount()));
        return this;
    }

    @Override
    public PostgresqlStatement bind(String identifier, Object value) {
        return bind(getIdentifierIndex(identifier), value);
    }

    @Override
    public PostgresqlStatement bind(int index, Object value) {
        Assert.requireNonNull(value, "value must not be null");

        BindingLogger.logBind(this.connectionContext, index, value);
        getCurrentOrFirstBinding().add(index, this.resources.getCodecs().encode(value));
        return this;
    }

    @Override
    public PostgresqlStatement bindNull(String identifier, Class<?> type) {
        return bindNull(getIdentifierIndex(identifier), type);
    }

    @Override
    public PostgresqlStatement bindNull(int index, Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (index >= tokenizedSql.getParameterCount()) {
            throw new UnsupportedOperationException(String.format("Cannot bind parameter %d, statement has %d parameters", index, this.tokenizedSql.getParameterCount()));
        }

        BindingLogger.logBindNull(this.connectionContext, index, type);
        getCurrentOrFirstBinding().add(index, this.resources.getCodecs().encodeNull(type));
        return this;
    }

    @Nonnull
    private Binding getCurrentOrFirstBinding() {
        Binding binding = bindings.peekLast();
        if (binding == null) {
            Binding newBinding = new Binding(tokenizedSql.getParameterCount());
            bindings.add(newBinding);
            return newBinding;
        } else {
            return binding;
        }
    }

    @Override
    public Flux<io.r2dbc.postgresql.api.PostgresqlResult> execute() {
        if (this.generatedColumns == null) {
            return execute(this.tokenizedSql.getSql());
        }
        return execute(GeneratedValuesUtils.augment(this.tokenizedSql.getSql(), this.generatedColumns));
    }

    @Override
    public PostgresqlStatement returnGeneratedValues(String... columns) {
        Assert.requireNonNull(columns, "columns must not be null");

        boolean hasReturning = this.tokenizedSql.hasDefaultTokenValue("RETURNING");
        if (hasReturning) {
            throw new IllegalStateException("Statement already includes RETURNING clause");
        }

        boolean isSupporting = this.tokenizedSql.hasDefaultTokenValue("DELETE", "INSERT", "UPDATE");
        if (!isSupporting) {
            throw new IllegalStateException("Statement is not a DELETE, INSERT, or UPDATE command");
        }

        this.generatedColumns = columns;
        return this;
    }

    @Override
    public PostgresqlStatement fetchSize(int rows) {
        Assert.isTrue(rows >= 0, "fetch size must be greater or equal zero");
        this.fetchSize = rows;
        return this;
    }

    @Override
    public String toString() {
        return "PostgresqlStatement{" +
            "bindings=" + this.bindings +
            ", context=" + this.resources +
            ", sql='" + this.tokenizedSql.getSql() + '\'' +
            ", generatedColumns=" + Arrays.toString(this.generatedColumns) +
            '}';
    }

    Binding getCurrentBinding() {
        return getCurrentOrFirstBinding();
    }

    private int getIdentifierIndex(String identifier) {
        Assert.requireNonNull(identifier, "identifier must not be null");
        Assert.requireType(identifier, String.class, "identifier must be a String");
        if (!identifier.startsWith("$")) {
            throw new NoSuchElementException(String.format("\"%s\" is not a valid identifier", identifier));
        }
        try {
            return Integer.parseInt(identifier.substring(1)) - 1;
        } catch (NumberFormatException e) {
            throw new NoSuchElementException(String.format("\"%s\" is not a valid identifier", identifier));
        }
    }

    private Flux<io.r2dbc.postgresql.api.PostgresqlResult> execute(String sql) {
        ExceptionFactory factory = ExceptionFactory.withSql(sql);

        if (this.tokenizedSql.getParameterCount() != 0) {
            // Extended query protocol
            if (this.bindings.size() == 0) {
                throw new IllegalStateException("No parameters have been bound");
            }

            this.bindings.forEach(Binding::validate);
            int fetchSize = this.fetchSize;
            return Flux.defer(() -> {

                // possible optimization: fetch all when statement is already prepared or first statement to be prepared
                if (this.bindings.size() == 1) {

                    Binding binding = this.bindings.peekFirst();
                    Flux<BackendMessage> messages = collectBindingParameters(binding).flatMapMany(values -> ExtendedFlowDelegate.runQuery(this.resources, factory, sql, binding, values, fetchSize));
                    return Flux.just(PostgresqlResult.toResult(this.resources, messages, factory));
                }

                Iterator<Binding> iterator = this.bindings.iterator();
                Sinks.Many<Binding> bindings = Sinks.many().unicast().onBackpressureBuffer();
                AtomicBoolean canceled = new AtomicBoolean();
                return bindings.asFlux()
                    .map(it -> {
                        Flux<BackendMessage> messages =
                            collectBindingParameters(it).flatMapMany(values -> ExtendedFlowDelegate.runQuery(this.resources, factory, sql, it, values, this.fetchSize)).doOnComplete(() -> tryNextBinding(iterator, bindings, canceled));

                        return PostgresqlResult.toResult(this.resources, messages, factory);
                    })
                    .doOnCancel(() -> clearBindings(iterator, canceled))
                    .doOnError(e -> clearBindings(iterator, canceled))
                    .doOnSubscribe(it -> bindings.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST));

            }).cast(io.r2dbc.postgresql.api.PostgresqlResult.class);
        } else {
            // Simple Query protocol
            if (this.fetchSize != NO_LIMIT) {
                return ExtendedFlowDelegate.runQuery(this.resources, factory, sql, Binding.EMPTY, Collections.emptyList(), this.fetchSize)
                    .windowUntil(WINDOW_UNTIL)
                    .map(messages -> PostgresqlResult.toResult(this.resources, messages, factory))
                    .as(Operators::discardOnCancel);
            }

            return SimpleQueryMessageFlow.exchange(this.resources.getClient(), sql)
                .windowUntil(WINDOW_UNTIL)
                .map(messages -> PostgresqlResult.toResult(this.resources, messages, factory))
                .as(Operators::discardOnCancel);
        }
    }

    private static void tryNextBinding(Iterator<Binding> iterator, Sinks.Many<Binding> bindingSink, AtomicBoolean canceled) {

        if (canceled.get()) {
            return;
        }

        try {
            if (iterator.hasNext()) {
                bindingSink.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
                bindingSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            }
        } catch (Exception e) {
            bindingSink.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }

    private static Mono<List<ByteBuf>> collectBindingParameters(Binding binding) {

        return Flux.fromIterable(binding.getParameterValues())
            .flatMap(f -> {
                if (f == EncodedParameter.NULL_VALUE) {
                    return Flux.just(Bind.NULL_VALUE);
                } else {
                    return Flux.from(f)
                        .reduce(Unpooled.compositeBuffer(), (c, b) -> c.addComponent(true, b));
                }
            })
            .collectList();
    }

    private void clearBindings(Iterator<Binding> iterator, AtomicBoolean canceled) {

        canceled.set(true);

        while (iterator.hasNext()) {
            // exhaust iterator, ignore returned elements
            iterator.next();
        }

        this.bindings.forEach(Binding::clear);
    }

}
