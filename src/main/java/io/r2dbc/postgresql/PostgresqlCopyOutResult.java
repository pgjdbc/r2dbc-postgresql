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

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.*;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BiFunction;

public class PostgresqlCopyOutResult extends AbstractReferenceCounted implements io.r2dbc.postgresql.api.PostgresqlCopyOutResult {

    private final Flux<BackendMessage> messages;

    private final ExceptionFactory factory;

    private volatile CopyOutMetadata metadata;

    PostgresqlCopyOutResult(Flux<BackendMessage> messages, ExceptionFactory factory) {
        this.messages = Assert.requireNonNull(messages, "messages must not be null");
        this.factory = Assert.requireNonNull(factory, "factory must not be null");
    }


    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Flux<T> map(BiFunction<ByteBuf, CopyOutMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return (Flux<T>) this.messages
                .handle((message, sink) -> {

                    try {
                        if (message instanceof ErrorResponse) {
                            this.factory.handleErrorResponse(message, (SynchronousSink) sink);
                            return;
                        }

                        if (message instanceof CopyOutResponse) {
                            this.metadata = PostgresCopyOutMetadata.toMetadata((CopyOutResponse) message);
                            return;
                        }

                        if (message instanceof CopyData) {
                            CopyData data = (CopyData) message;
                            sink.next(f.apply(data.getData(), this.metadata));
                            return;
                        }

                        if (message instanceof CopyDone) {
                            sink.complete();
                            return;
                        }

                        if (message instanceof RowDescription) {
                            sink.error(new IllegalStateException("copyOut may only be used with statements that execute a COPY ... TO STDOUT query"));
                        }

                    } finally {
                        ReferenceCountUtil.release(message);
                    }
                });
    }

    @Override
    protected void deallocate() {
        // drain messages for cleanup
        messages.map(ReferenceCountUtil::release).subscribe();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    public String toString() {
        return "PostgresqlCopyOutResult{" +
                "messages=" + messages +
                ", factory=" + factory +
                '}';
    }

    static PostgresqlCopyOutResult toCopyOutResult(Flux<BackendMessage> messages, ExceptionFactory factory) {
        return new PostgresqlCopyOutResult(messages, factory);
    }

    private static class PostgresCopyOutMetadata implements CopyOutMetadata {
        private final Format overallFormat;
        private final Collection<Format> columnFormats;

        private PostgresCopyOutMetadata(Format overallFormat, Collection<Format> columnFormats) {
            this.overallFormat = overallFormat;
            this.columnFormats = columnFormats;
        }

        @Override
        public Format getOverallFormat() {
            return overallFormat;
        }

        public Collection<Format> getColumnFormats() {
            return columnFormats;
        }

        @Override
        public String toString() {
            return "PostgresCopyOutMetadata{" +
                    "overallFormat=" + overallFormat +
                    ", columnFormats=" + columnFormats +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CopyOutMetadata)) return false;
            CopyOutMetadata that = (CopyOutMetadata) o;
            return overallFormat == that.getOverallFormat() && Objects.equals(columnFormats, that.getColumnFormats());
        }

        @Override
        public int hashCode() {
            return Objects.hash(overallFormat, columnFormats);
        }

        static CopyOutMetadata toMetadata(CopyOutResponse response) {
            return new PostgresCopyOutMetadata(response.getOverallFormat(), response.getColumnFormats());
        }
    }
}
