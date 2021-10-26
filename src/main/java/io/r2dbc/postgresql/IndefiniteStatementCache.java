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

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.util.Assert;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class IndefiniteStatementCache implements StatementCache {

    private final Map<String, Map<int[], String>> cache = new ConcurrentHashMap<>();

    private final AtomicInteger counter = new AtomicInteger();

    IndefiniteStatementCache() {
    }

    @Override
    public String getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        Map<int[], String> typedMap = getTypeMap(sql);
        String name = typedMap.get(binding.getParameterTypes());

        if (name == null) {
            name = "S_" + this.counter.getAndIncrement();
        }

        return name;
    }

    @Override
    public boolean requiresPrepare(Binding binding, String sql) {

        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        Map<int[], String> typedMap = getTypeMap(sql);
        return !typedMap.containsKey(binding.getParameterTypes());
    }

    @Override
    public void put(Binding binding, String sql, String name) {

        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        Map<int[], String> typedMap = getTypeMap(sql);

        typedMap.put(binding.getParameterTypes(), name);
    }

    @Override
    public void evict(String sql) {
        this.cache.remove(sql);
    }

    private Map<int[], String> getTypeMap(String sql) {

        return this.cache.computeIfAbsent(sql, ignore -> new TreeMap<>((o1, o2) -> {

            if (Arrays.equals(o1, o2)) {
                return 0;
            }

            if (o1.length != o2.length) {
                return o1.length - o2.length;
            }

            for (int i = 0; i < o1.length; i++) {

                int cmp = Integer.compare(o1[i], o2[i]);

                if (cmp != 0) {
                    return cmp;
                }
            }

            return 0;
        }));
    }

    @Override
    public String toString() {
        return "IndefiniteStatementCache{" +
            "cache=" + this.cache +
            ", counter=" + this.counter +
            '}';
    }

}
