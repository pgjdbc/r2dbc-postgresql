/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;

final class BlobCodec extends AbstractCodec<Blob> {

    private final ByteBufAllocator byteBufAllocator;

    BlobCodec(ByteBufAllocator byteBufAllocator) {
        super(Blob.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(BYTEA, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return BYTEA == type;
    }

    @Override
    Blob doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends Blob> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return new ByteABlob(buffer, format);
    }

    @Override
    EncodedParameter doEncode(Blob value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(BYTEA, FORMAT_TEXT,
            Flux.from(value.stream())
                .reduce(this.byteBufAllocator.compositeBuffer(), (a, b) -> a.addComponent(true, Unpooled.wrappedBuffer(b)))
                .map(it -> AbstractBinaryCodec.encodeToHex(it, this.byteBufAllocator))
                .concatWith(Flux.from(value.discard())
                    .then(Mono.empty()))
        );
    }

    private static final class ByteABlob implements Blob {

        private final ByteBuf byteBuf;

        private final Format format;

        private ByteABlob(ByteBuf byteBuf, Format format) {
            this.byteBuf = byteBuf.retain();
            this.format = format;
        }

        @Override
        public Mono<Void> discard() {
            return Mono.fromRunnable(() -> {
                if (this.byteBuf.refCnt() > 0) {
                    this.byteBuf.release();
                }
            });
        }

        @Override
        public Mono<ByteBuffer> stream() {
            return Mono.fromSupplier(() -> {
                if (this.format == Format.FORMAT_BINARY) {
                    return this.byteBuf.nioBuffer();
                }

                return ByteBuffer.wrap(AbstractBinaryCodec.decodeFromHex(this.byteBuf));
            }).doAfterTerminate(this.byteBuf::release);
        }

    }

}
