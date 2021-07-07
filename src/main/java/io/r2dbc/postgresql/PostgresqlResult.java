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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An implementation of {@link Result} representing the results of a query against a PostgreSQL database applying fast-path processing. Processing of {@link Segment} is handled entirely by
 * {@link PostgresqlSegmentResult}.
 */
final class PostgresqlResult extends AbstractReferenceCounted implements io.r2dbc.postgresql.api.PostgresqlResult {

    private final ConnectionResources resources;

    private final Flux<BackendMessage> messages;

    private final ExceptionFactory factory;

    private volatile PostgresqlRowMetadata metadata;

    private volatile RowDescription rowDescription;

    PostgresqlResult(ConnectionResources resources, Flux<BackendMessage> messages, ExceptionFactory factory) {
        this.resources = Assert.requireNonNull(resources, "resources must not be null");
        this.messages = Assert.requireNonNull(messages, "messages must not be null");
        this.factory = Assert.requireNonNull(factory, "factory must not be null");
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Mono<Integer> getRowsUpdated() {

        return this.messages
            .<Integer>handle((message, sink) -> {

                if (message instanceof ErrorResponse) {
                    this.factory.handleErrorResponse(message, (SynchronousSink) sink);
                    return;
                }

                if (message instanceof DataRow) {
                    ((DataRow) message).release();
                }

                if (message instanceof CommandComplete) {

                    Integer rowCount = ((CommandComplete) message).getRows();
                    if (rowCount != null) {
                        sink.next(rowCount);
                    }
                }
            }).singleOrEmpty();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.messages
            .handle((message, sink) -> {

                try {
                    if (message instanceof ErrorResponse) {
                        this.factory.handleErrorResponse(message, (SynchronousSink) sink);
                        return;
                    }

                    if (message instanceof RowDescription) {
                        this.rowDescription = (RowDescription) message;
                        this.metadata = PostgresqlRowMetadata.toRowMetadata(this.resources.getCodecs(), (RowDescription) message);
                        return;
                    }

                    if (message instanceof DataRow) {
                        PostgresqlRow row = PostgresqlRow.toRow(this.resources, (DataRow) message, this.metadata, this.rowDescription);
                        sink.next(f.apply(row, this.metadata));
                    }

                } finally {
                    ReferenceCountUtil.release(message);
                }
            });
    }

    @Override
    public io.r2dbc.postgresql.api.PostgresqlResult filter(Predicate<Segment> filter) {
        return PostgresqlSegmentResult.toResult(this.resources, this.messages, this.factory).filter(filter);
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        return PostgresqlSegmentResult.toResult(this.resources, this.messages, this.factory).flatMap(mappingFunction);
    }

    @Override
    protected void deallocate() {

        // drain messages for cleanup
        this.getRowsUpdated().subscribe();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    public String toString() {
        return "PostgresqlResult{" +
            "context=" + this.resources +
            ", messages=" + this.messages +
            '}';
    }

    static PostgresqlResult toResult(ConnectionResources resources, Flux<BackendMessage> messages, ExceptionFactory factory) {
        return new PostgresqlResult(resources, messages, factory);
    }

}
