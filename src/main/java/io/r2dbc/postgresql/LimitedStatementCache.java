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
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitedStatementCache implements StatementCache {

    private final LinkedHashMap<Tuple2<String, List<Integer>>, String> cache = new LinkedHashMap<>(16, 0.75f, true);

    private final Client client;

    private final AtomicInteger counter = new AtomicInteger();

    private final int limit;

    public LimitedStatementCache(Client client, int limit) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("statement cache limit must be greater than zero");
        }
        this.limit = limit;
    }

    @Override
    public Mono<String> getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");
        Tuple2<String, List<Integer>> key = Tuples.of(sql, binding.getParameterTypes());
        String name = this.cache.get(key);
        if (name != null) {
            return Mono.just(name);
        }

        Mono<Void> closeLastStatement = Mono.defer(() -> {
            if (this.cache.size() < this.limit) {
                return Mono.empty();
            }
            String lastAccessedStatementName = this.cache.values().iterator().next();
            ExceptionFactory factory = ExceptionFactory.withSql(lastAccessedStatementName);
            return ExtendedQueryMessageFlow
                .closeStatement(this.client, lastAccessedStatementName)
                .handle(factory::handleErrorResponse)
                .then();
        });

        return closeLastStatement.then(this.parse(sql, binding.getParameterTypes()))
            .doOnNext(preparedName -> this.cache.put(key, preparedName));
    }

    @Override
    public String toString() {
        return "LimitedStatementCache{" +
            "cache=" + this.cache +
            ", counter=" + this.counter +
            ", client=" + this.client +
            ", limit=" + this.limit +
            '}';
    }

    private Mono<String> parse(String sql, List<Integer> types) {
        String name = String.format("S_%d", this.counter.getAndIncrement());

        ExceptionFactory factory = ExceptionFactory.withSql(name);
        return ExtendedQueryMessageFlow
            .parse(this.client, name, sql, types)
            .handle(factory::handleErrorResponse)
            .then(Mono.just(name))
            .cache();
    }
}
