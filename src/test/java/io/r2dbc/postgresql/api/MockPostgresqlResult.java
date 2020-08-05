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

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class MockPostgresqlResult implements PostgresqlResult {

    private final Mono<RowMetadata> rowMetadata;

    private final Flux<Row> rows;

    public MockPostgresqlResult(Mono<RowMetadata> rowMetadata, Flux<Row> rows) {
        this.rowMetadata = rowMetadata;
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
            .zipWith(this.rowMetadata.repeat())
            .map((tuple) -> {
                Row row = tuple.getT1();
                RowMetadata rowMetadata = tuple.getT2();

                return mappingFunction.apply(row, rowMetadata);
            });
    }

    public static final class Builder {

        private final List<Row> rows = new ArrayList<>();

        private RowMetadata rowMetadata;

        private Builder() {
        }

        public MockPostgresqlResult build() {
            return new MockPostgresqlResult(Mono.justOrEmpty(this.rowMetadata), Flux.fromIterable(this.rows));
        }

        public Builder rowMetadata(RowMetadata rowMetadata) {
            this.rowMetadata = rowMetadata;
            return this;
        }

        public Builder row(Row row) {
            this.rows.add(row);
            return this;
        }

    }

}
