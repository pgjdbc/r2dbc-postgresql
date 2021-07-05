/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.postgresql.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public final class MockPostgresqlResult implements PostgresqlResult {

    private final Flux<Row> rows;

    public MockPostgresqlResult(Flux<Row> rows) {
        this.rows = rows;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return Mono.empty();
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        return this.rows
            .map((row) -> {

                return mappingFunction.apply(row, row.getMetadata());
            });
    }

    @Override
    public Result filter(Predicate<Segment> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        throw new UnsupportedOperationException();
    }

    public static final class Builder {

        private final List<Row> rows = new ArrayList<>();

        private Builder() {
        }

        public MockPostgresqlResult build() {
            return new MockPostgresqlResult(Flux.fromIterable(this.rows));
        }

        public Builder row(Row row) {
            this.rows.add(row);
            return this;
        }

    }

}
