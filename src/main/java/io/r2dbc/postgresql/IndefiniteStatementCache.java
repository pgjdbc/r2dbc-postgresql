/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class IndefiniteStatementCache implements StatementCache {

    private final Map<String, Map<int[], Mono<String>>> cache = new ConcurrentHashMap<>();

    private final Client client;

    private final AtomicInteger counter = new AtomicInteger();

    IndefiniteStatementCache(Client client) {
        this.client = Assert.requireNonNull(client, "client must not be null");
    }

    @Override
    public Mono<String> getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");
        Map<int[], Mono<String>> typedMap = this.cache.computeIfAbsent(sql, ignore -> new TreeMap<>((o1, o2) -> {

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

        Mono<String> mono = typedMap.get(binding.getParameterTypes());
        if (mono == null) {
            mono = this.parse(sql, binding.getParameterTypes());
            typedMap.put(binding.getParameterTypes(), mono);
        }

        return mono;
    }

    @Override
    public String toString() {
        return "IndefiniteStatementCache{" +
            "cache=" + this.cache +
            ", client=" + this.client +
            ", counter=" + this.counter +
            '}';
    }

    private Mono<String> parse(String sql, int[] types) {
        String name = "S_" + this.counter.getAndIncrement();

        ExceptionFactory factory = ExceptionFactory.withSql(sql);
        return ExtendedQueryMessageFlow
            .parse(this.client, name, sql, types)
            .handle(factory::handleErrorResponse)
            .then(Mono.just(name))
            .cache();
    }
}
