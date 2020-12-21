/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BYTEA;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Support class for codecs that read/write {@code BYTEA} values.
 *
 * @param <T> the type that is handled by this {@link Codec}
 */
abstract class AbstractBinaryCodec<T> extends AbstractCodec<T> {

    private static final Pattern BLOB_PATTERN = Pattern.compile("\\\\x([\\p{XDigit}]+)?");

    private final ByteBufAllocator byteBufAllocator;

    AbstractBinaryCodec(Class<T> type, ByteBufAllocator byteBufAllocator) {
        super(type);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(FORMAT_TEXT, BYTEA);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");

        return BYTEA == type;
    }

    @Override
    EncodedParameter doEncode(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return doEncode(value, BYTEA);
    }

    byte[] decode(Format format, ByteBuf byteBuf) {
        if (format == FORMAT_TEXT) {
            return decodeFromHex(byteBuf);
        }

        byte[] decoded = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(decoded);

        return decoded;
    }

    ByteBuf encodeToHex(ByteBuf value) {
        return encodeToHex(value, this.byteBufAllocator);
    }

    static ByteBuf encodeToHex(ByteBuf value, ByteBufAllocator byteBufAllocator) {

        int blobSize = value.readableBytes();
        ByteBuf buf = byteBufAllocator.buffer(2 + blobSize * 2);
        buf.writeByte('\\').writeByte('x');

        int chunkSize = 1024;

        while (value.isReadable()) {
            chunkSize = Math.min(chunkSize, value.readableBytes());
            buf.writeCharSequence(ByteBufUtil.hexDump(value, value.readerIndex(), chunkSize), StandardCharsets.US_ASCII);
            value.skipBytes(chunkSize);
        }

        // release value after encoding as it is consumed.
        value.release();

        return buf;
    }

    static byte[] decodeFromHex(ByteBuf byteBuf) {
        Matcher matcher = BLOB_PATTERN.matcher(ByteBufUtils.decode(byteBuf));

        if (!matcher.find()) {
            throw new IllegalStateException("ByteBuf does not contain BYTEA hex format");
        }

        String bytesHex = Objects.toString(matcher.group(1), "");
        return ByteBufUtil.decodeHexDump(bytesHex);
    }

}
