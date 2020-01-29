/*
 * Copyright 2019-2020 the original author or authors.
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
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded (size-limited) {@link StatementCache}.
 */
final class BoundedStatementCache implements StatementCache {

    private final Map<Tuple2<String, List<Integer>>, String> cache = new LinkedHashMap<>(16, 0.75f, true);

    private final Client client;

    private final AtomicInteger counter = new AtomicInteger();

    private final int limit;

    public BoundedStatementCache(Client client, int limit) {
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
        String name = get(key);
        if (name != null) {
            return Mono.just(name);
        }

        Mono<Void> closeLastStatement = Mono.defer(() -> {
            if (getCacheSize() < this.limit) {
                return Mono.empty();
            }
            String lastAccessedStatementName = getAndRemoveEldest();
            ExceptionFactory factory = ExceptionFactory.withSql(lastAccessedStatementName);
            return ExtendedQueryMessageFlow
                .closeStatement(this.client, lastAccessedStatementName)
                .handle(factory::handleErrorResponse)
                .then();
        });

        return closeLastStatement.then(this.parse(sql, binding.getParameterTypes()))
            .doOnNext(preparedName -> put(key, preparedName));
    }


    /**
     * Synchronized cache access: Return all statement names.
     *
     * @return statement names.
     */
    Collection<String> getCachedStatementNames() {
        synchronized (this.cache) {
            List<String> names = new ArrayList<>(this.cache.size());
            names.addAll(cache.values());
            return names;
        }
    }

    /**
     * Synchronized cache access: Retrieve statement name by key.
     *
     * @param key
     * @return statement name by key
     */
    @Nullable
    private String get(Tuple2<String, List<Integer>> key) {
        synchronized (this.cache) {
            return this.cache.get(key);
        }
    }

    /**
     * Synchronized cache access: Least recently used entry.
     *
     * @return least recently used entry
     */
    private String getAndRemoveEldest() {
        synchronized (this.cache) {
            Iterator<Map.Entry<Tuple2<String, List<Integer>>, String>> iterator = this.cache.entrySet().iterator();
            String entry = iterator.next().getValue();
            iterator.remove();
            return entry;
        }
    }

    /**
     * Synchronized cache access: Store prepared statement.
     */
    private void put(Tuple2<String, List<Integer>> key, String preparedName) {
        synchronized (this.cache) {
            this.cache.put(key, preparedName);
        }
    }

    /**
     * Synchronized cache access: Return the cache size.
     *
     * @return the cache size.
     */
    private int getCacheSize() {
        synchronized (this.cache) {
            return this.cache.size();
        }
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
