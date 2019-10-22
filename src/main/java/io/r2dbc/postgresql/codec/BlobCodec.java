/*
 * Copyright 2017-2019 the original author or authors.
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import io.r2dbc.spi.Blob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;

final class BlobCodec extends AbstractCodec<Blob> {

    private final ByteBufAllocator byteBufAllocator;

    BlobCodec(ByteBufAllocator byteBufAllocator) {
        super(Blob.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
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
    Parameter doEncode(Blob value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(BYTEA, FORMAT_TEXT,
            Flux.from(value.stream())
                .reduce(this.byteBufAllocator.compositeBuffer(), (a, b) -> a.addComponent(true, Unpooled.wrappedBuffer(b)))
                .map(this::toHexFormat)
                .concatWith(Flux.from(value.discard())
                    .then(Mono.empty()))
        );
    }

    private ByteBuf toHexFormat(ByteBuf b) {
        int blobSize = b.readableBytes();
        ByteBuf buf = this.byteBufAllocator.buffer(2 + blobSize * 2);
        buf.writeByte('\\').writeByte('x');

        int chunkSize = 1024;

        while (b.isReadable()) {
            chunkSize = Math.min(chunkSize, b.readableBytes());
            buf.writeCharSequence(ByteBufUtil.hexDump(b, b.readerIndex(), chunkSize), StandardCharsets.US_ASCII);
            b.skipBytes(chunkSize);
        }
        b.release();

        return buf;
    }

    private static final class ByteABlob implements Blob {

        private static final Pattern BLOB_PATTERN = Pattern.compile("\\\\x([\\p{XDigit}]+)");

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

                Matcher matcher = BLOB_PATTERN.matcher(ByteBufUtils.decode(this.byteBuf));

                if (!matcher.find()) {
                    throw new IllegalStateException("ByteBuf does not contain BYTEA hex format");
                }

                return ByteBuffer.wrap(ByteBufUtil.decodeHexDump(matcher.group(1)));
            }).doAfterTerminate(this.byteBuf::release);
        }
    }

}
