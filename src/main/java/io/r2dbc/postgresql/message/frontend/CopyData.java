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

import java.nio.ByteBuffer;
import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.MESSAGE_OVERHEAD;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeBytes;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The CopyData message.
 */
public final class CopyData implements FrontendMessage {

    private final ByteBuffer data;

    /**
     * Creates a new message.
     *
     * @param data data that forms part of a {@code COPY} data stream
     * @throws IllegalArgumentException if {@code data} is {@code null}
     */
    public CopyData(ByteBuffer data) {
        Assert.requireNonNull(data, "data must not be null");

        this.data = (ByteBuffer) data.flip();
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.defer(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer(MESSAGE_OVERHEAD + (this.data.remaining()));

            writeByte(out, 'd');
            writeLengthPlaceholder(out);
            writeBytes(out, this.data);

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
        CopyData copyData = (CopyData) o;
        return Objects.equals(this.data, copyData.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data);
    }

    @Override
    public String toString() {
        return "CopyData{" +
            "data=" + this.data +
            '}';
    }

}
