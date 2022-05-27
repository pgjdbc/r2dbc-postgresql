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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.api.CopyInBuilder;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.CopyData;
import io.r2dbc.postgresql.message.frontend.CopyDone;
import io.r2dbc.postgresql.message.frontend.CopyFail;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.Operators;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.r2dbc.postgresql.PostgresqlResult.toResult;

/**
 * An implementation for {@link CopyData} PostgreSQL queries.
 */
final class PostgresqlCopyIn {

    private final ConnectionResources context;

    PostgresqlCopyIn(ConnectionResources resources) {
        this.context = Assert.requireNonNull(resources, "resources must not be null");
    }

    Mono<Long> copy(String sql, Publisher<? extends Publisher<ByteBuf>> stdin) {

        ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);

        return Flux.from(stdin)
            .<FrontendMessage>concatMap(data -> {

                CompositeByteBuf composite = this.context.getClient().getByteBufAllocator().compositeBuffer();

                return Flux.from(data)
                    .reduce(composite, (l, r) -> l.addComponent(true, r))
                    .map(CopyData::new)
                    .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release);

            }).concatWithValues(CopyDone.INSTANCE).startWith(new Query(sql))
            .as(messages -> copyIn(exceptionFactory, messages));
    }

    private Mono<Long> copyIn(ExceptionFactory exceptionFactory, Flux<FrontendMessage> copyDataMessages) {

        Client client = this.context.getClient();
        AtomicBoolean stop = new AtomicBoolean();
        Sinks.Many<FrontendMessage> sink = Sinks.many().unicast().onBackpressureBuffer();
        Flux<FrontendMessage> requestMessages = sink.asFlux().mergeWith(copyDataMessages
            .doOnComplete(sink::tryEmitComplete)
            .filter(it -> !stop.get())
            .onErrorResume(e -> {
                copyFail(sink, stop, "Copy operation failed: " + e.getMessage());
                return Mono.empty();
            }));

        return client.exchange(backendMessage -> backendMessage instanceof ReadyForQuery, requestMessages)
            .doOnNext(it -> {
                if (it instanceof ErrorResponse) {
                    stop.set(true);
                    sink.tryEmitComplete();
                }
            })
            .doOnComplete(() -> {
                stop.set(true);
                sink.tryEmitComplete();
            })
            .doOnError((e) -> {
                copyFail(sink, stop, "Copy operation failed: " + e.getMessage());
            })
            .doOnCancel(() -> {
                copyFail(sink, stop, "Copy operation failed: Cancelled");
            })
            .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
            .as(Operators::discardOnCancel)
            .doOnCancel(() -> {
                copyFail(sink, stop, "Copy operation failed: Cancelled");
            })
            .as(messages -> toResult(this.context, messages, exceptionFactory).getRowsUpdated());
    }

    private void copyFail(Sinks.Many<FrontendMessage> sink, AtomicBoolean stop, String e) {
        sink.tryEmitNext(new CopyFail(e));
        sink.tryEmitComplete();
        stop.set(true);
    }

    @Override
    public String toString() {
        return "PostgresqlCopyIn{" +
            "context=" + this.context +
            '}';
    }

    static final class Builder implements CopyInBuilder {

        private final ConnectionResources resources;

        private final String sql;

        @Nullable
        private Publisher<? extends Publisher<ByteBuf>> stdin;

        Builder(ConnectionResources resources, String sql) {
            this.resources = resources;
            this.sql = sql;
        }

        @Override
        public CopyInBuilder fromMany(Publisher<? extends Publisher<ByteBuf>> stdin) {
            this.stdin = Assert.requireNonNull(stdin, "stdin must not be null");
            return this;
        }

        @Override
        public Mono<Long> build() {

            if (this.stdin == null) {
                throw new IllegalArgumentException("No stdin configured for COPY IN");
            }

            return new PostgresqlCopyIn(this.resources).copy(this.sql, this.stdin);
        }

    }

}
