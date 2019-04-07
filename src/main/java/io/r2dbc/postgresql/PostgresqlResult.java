/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

import static reactor.function.TupleUtils.function;

/**
 * An implementation of {@link Result} representing the results of a query against a PostgreSQL database.
 */
public final class PostgresqlResult implements Result {

    private final Codecs codecs;

    private final Mono<PostgresqlRowMetadata> rowMetadata;

    private final Flux<PostgresqlRow> rows;

    private final Mono<Integer> rowsUpdated;

    PostgresqlResult(Codecs codecs, Mono<PostgresqlRowMetadata> rowMetadata, Flux<PostgresqlRow> rows, Mono<Integer> rowsUpdated) {
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.rowMetadata = Assert.requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.rows = Assert.requireNonNull(rows, "rows must not be null");
        this.rowsUpdated = Assert.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return this.rowsUpdated;
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.rows
            .zipWith(this.rowMetadata.repeat())
            .map(function((row, rowMetadata) -> {
                try {
                    return f.apply(row, rowMetadata);
                } finally {
                    row.release();
                }
            }));
    }

    @Override
    public String toString() {
        return "PostgresqlResult{" +
            "codecs=" + this.codecs +
            ", rowMetadata=" + this.rowMetadata +
            ", rows=" + this.rows +
            ", rowsUpdated=" + this.rowsUpdated +
            '}';
    }

    static PostgresqlResult toResult(Codecs codecs, Flux<BackendMessage> messages) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(messages, "messages must not be null");

        EmitterProcessor<BackendMessage> processor = EmitterProcessor.create(false);
        Flux<BackendMessage> firstMessages = processor.take(3).cache();

        Mono<RowDescription> rowDescription = firstMessages
            .ofType(RowDescription.class)
            .singleOrEmpty()
            .cache();

        Mono<PostgresqlRowMetadata> rowMetadata = rowDescription
            .map(d -> PostgresqlRowMetadata.toRowMetadata(codecs, d));

        Flux<PostgresqlRow> rows = processor
            .startWith(firstMessages)
            .ofType(DataRow.class)
            .zipWith(rowDescription.repeat())
            .map(function((dataRow, rd) -> PostgresqlRow.toRow(codecs, dataRow, rd)));

        Mono<Integer> rowsUpdated = firstMessages
            .ofType(CommandComplete.class)
            .singleOrEmpty()
            .handle((commandComplete, sink) -> {
                Integer rowCount = commandComplete.getRows();
                if (rowCount != null) {
                    sink.next(rowCount);
                }
                else {
                    sink.complete();
                }
            });

        messages
            .handle(PostgresqlServerErrorException::handleErrorResponse)
            .hide()
            .subscribe(processor);

        return new PostgresqlResult(codecs, rowMetadata, rows, rowsUpdated);
    }

}
