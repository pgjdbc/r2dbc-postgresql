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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Query message.
 */
public final class Query implements FrontendMessage {

    private final String query;

    /**
     * Creates a new message.
     *
     * @param query the query string
     * @throws IllegalArgumentException if {@code query} is {@code null}
     */
    public Query(String query) {
        this.query = Assert.requireNonNull(query, "query must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.defer(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();

            writeByte(out, 'Q');
            writeLengthPlaceholder(out);
            writeCStringUTF8(out, this.query);

            return Mono.just(writeSize(out));
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Query that = (Query) o;
        return Objects.equals(this.query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.query);
    }

    @Override
    public String toString() {
        return "Query{" +
            "query='" + this.query + '\'' +
            '}';
    }

}
