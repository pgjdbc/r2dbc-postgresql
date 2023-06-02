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
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
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

    private static final Logger LOGGER = Loggers.getLogger(BoundedStatementCache.class);

    private final Map<CacheKey, String> cache = new LinkedHashMap<>(16, 0.75f, true);

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
    public String getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        String name = get(new CacheKey(sql, binding.getParameterTypes()));

        if (name != null) {
            return name;
        }

        return "S_" + this.counter.getAndIncrement();
    }

    @Override
    public boolean requiresPrepare(Binding binding, String sql) {

        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        return get(new CacheKey(sql, binding.getParameterTypes())) == null;
    }

    @Override
    public void put(Binding binding, String sql, String name) {

        CacheKey key = new CacheKey(sql, binding.getParameterTypes());

        put(key, name);

        if (getCacheSize() <= this.limit) {
            return;
        }

        Map.Entry<CacheKey, String> lastAccessedStatement = getAndRemoveEldest();
        ExceptionFactory factory = ExceptionFactory.withSql(lastAccessedStatement.getKey().sql);
        String statementName = lastAccessedStatement.getValue();

        close(lastAccessedStatement, factory, statementName);
    }

    @Override
    public void evict(String name) {

        synchronized (this.cache) {

            List<CacheKey> toRemove = new ArrayList<>();
            for (Map.Entry<CacheKey, String> entry : this.cache.entrySet()) {
                if (entry.getKey().sql.equals(name)) {
                    toRemove.add(entry.getKey());
                }
            }

            toRemove.forEach(this.cache::remove);
        }
    }

    private void close(Map.Entry<CacheKey, String> lastAccessedStatement, ExceptionFactory factory, String statementName) {
        ExtendedQueryMessageFlow
            .closeStatement(this.client, statementName)
            .handle(factory::handleErrorResponse)
            .subscribe(it -> {
            }, err -> LOGGER.warn(String.format("Cannot close statement %s (%s)", lastAccessedStatement.getValue(), lastAccessedStatement.getKey().sql), err));
    }

    /**
     * Synchronized cache access: Return all statement names.
     *
     * @return statement names.
     */
    Collection<String> getCachedStatementNames() {
        synchronized (this.cache) {
            List<String> names = new ArrayList<>(this.cache.size());
            names.addAll(this.cache.values());
            return names;
        }
    }

    /**
     * Synchronized cache access: Retrieve statement name by key.
     *
     * @param key Cache key
     * @see CacheKey
     * @return statement name by key
     */
    @Nullable
    private String get(CacheKey key) {
        synchronized (this.cache) {
            return this.cache.get(key);
        }
    }

    /**
     * Synchronized cache access: Least recently used entry.
     *
     * @return least recently used entry
     */
    private Map.Entry<CacheKey, String> getAndRemoveEldest() {
        synchronized (this.cache) {
            Iterator<Map.Entry<CacheKey, String>> iterator = this.cache.entrySet().iterator();
            Map.Entry<CacheKey, String> entry = iterator.next();
            iterator.remove();
            return entry;
        }
    }

    /**
     * Synchronized cache access: Store prepared statement.
     */
    private void put(CacheKey key, String preparedName) {
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

    static class CacheKey {

        String sql;

        int[] parameterTypes;

        public CacheKey(String sql, int[] parameterTypes) {
            this.sql = sql;
            this.parameterTypes = parameterTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }

            CacheKey cacheKey = (CacheKey) o;

            if (this.sql != null ? !this.sql.equals(cacheKey.sql) : cacheKey.sql != null) {
                return false;
            }
            return Arrays.equals(this.parameterTypes, cacheKey.parameterTypes);
        }

        @Override
        public int hashCode() {
            int result = this.sql != null ? this.sql.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(this.parameterTypes);
            return result;
        }

    }

}
