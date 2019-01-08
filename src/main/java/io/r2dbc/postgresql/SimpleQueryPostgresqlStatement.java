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
import io.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.client.ExtendedQueryMessageFlow.PARAMETER_SYMBOL;
import static io.r2dbc.postgresql.util.PredicateUtils.or;

final class SimpleQueryPostgresqlStatement implements PostgresqlStatement<SimpleQueryPostgresqlStatement> {

    private final Client client;

    private final Codecs codecs;

    private final String sql;

    SimpleQueryPostgresqlStatement(Client client, Codecs codecs, String sql) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.sql = Assert.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public SimpleQueryPostgresqlStatement add() {
        return this;
    }

    @Override
    public SimpleQueryPostgresqlStatement bind(@Nullable Object identifier, @Nullable Object value) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bind(int index, @Nullable Object value) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bindNull(@Nullable Object identifier, @Nullable Class<?> type) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public SimpleQueryPostgresqlStatement bindNull(int index, @Nullable Class<?> type) {
        throw new UnsupportedOperationException(String.format("Binding parameters is not supported for the statement '%s'", this.sql));
    }

    @Override
    public Flux<PostgresqlResult> execute() {
        return SimpleQueryMessageFlow
            .exchange(this.client, this.sql)
            .windowUntil(or(CommandComplete.class::isInstance, EmptyQueryResponse.class::isInstance, ErrorResponse.class::isInstance))
            .map(dataRow -> PostgresqlResult.toResult(this.codecs, dataRow));
    }

    @Override
    public String toString() {
        return "SimpleQueryPostgresqlStatement{" +
            "client=" + this.client +
            ", codecs=" + this.codecs +
            ", sql='" + this.sql + '\'' +
            '}';
    }

    static boolean supports(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        return sql.trim().isEmpty() || !PARAMETER_SYMBOL.matcher(sql).matches();
    }

}
