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

import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.PortalSuspended;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.util.PredicateUtils.or;

/**
 * An implementation of {@link Result} representing the results of a query against a PostgreSQL database.
 */
final class PostgresqlResult implements io.r2dbc.postgresql.api.PostgresqlResult {

    private static final Predicate<BackendMessage> TAKE_UNTIL = or(CommandComplete.class::isInstance, EmptyQueryResponse.class::isInstance, PortalSuspended.class::isInstance);

    private final Codecs codecs;

    private final Flux<BackendMessage> messages;

    private volatile PostgresqlRowMetadata metadata;

    private volatile RowDescription rowDescription;

    PostgresqlResult(Codecs codecs, Flux<BackendMessage> messages) {
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.messages = Assert.requireNonNull(messages, "messages must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {

        return this.messages
            .doOnNext(ReferenceCountUtil::release)
            .ofType(CommandComplete.class)
            .singleOrEmpty()
            .handle((commandComplete, sink) -> {
                Integer rowCount = commandComplete.getRows();
                if (rowCount != null) {
                    sink.next(rowCount);
                } else {
                    sink.complete();
                }
            });
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.messages.takeUntil(TAKE_UNTIL)
            .handle((message, sink) -> {

                try {
                    if (message instanceof RowDescription) {
                        this.rowDescription = (RowDescription) message;
                        this.metadata = PostgresqlRowMetadata.toRowMetadata(this.codecs, (RowDescription) message);
                        return;
                    }

                    if (message instanceof DataRow) {
                        PostgresqlRow row = PostgresqlRow.toRow(this.codecs, (DataRow) message, this.rowDescription);


                        sink.next(f.apply(row, this.metadata));
                    }

                } finally {
                    ReferenceCountUtil.release(message);
                }
            });
    }

    @Override
    public String toString() {
        return "PostgresqlResult{" +
            "codecs=" + this.codecs +
            ", messages=" + this.messages +
            '}';
    }

    static PostgresqlResult toResult(Codecs codecs, Flux<BackendMessage> messages) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(messages, "messages must not be null");

        return new PostgresqlResult(codecs, messages);
    }

}
