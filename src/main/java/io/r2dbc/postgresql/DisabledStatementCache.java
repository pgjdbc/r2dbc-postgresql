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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ExtendedQueryMessageFlow;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

class DisabledStatementCache implements StatementCache {

    private static final String UNNAMED_STATEMENT_NAME = "";

    private final Client client;

    DisabledStatementCache(Client client) {
        this.client = Assert.requireNonNull(client, "client must not be null");
    }

    @Override
    public Mono<String> getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");
        String name = UNNAMED_STATEMENT_NAME;

        ExceptionFactory factory = ExceptionFactory.withSql(name);
        return ExtendedQueryMessageFlow
            .parse(this.client, name, sql, binding.getParameterTypes())
            .handle(factory::handleErrorResponse)
            .then(Mono.just(name));
    }

    @Override
    public String toString() {
        return "DisabledStatementCache{" +
            "client=" + this.client +
            '}';
    }
}
