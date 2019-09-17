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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;

abstract class AbstractBinaryCodec<T> extends AbstractCodec<T> {

    private static final Pattern BLOB_PATTERN = Pattern.compile("\\\\x([\\p{XDigit}]+)");

    private final ByteBufAllocator byteBufAllocator;

    AbstractBinaryCodec(Class<T> type, ByteBufAllocator byteBufAllocator) {
        super(type);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(BYTEA, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");

        return BYTEA == type;
    }

    byte[] decode(Format format, ByteBuf byteBuf) {
        byte[] decoded;
        if (format == FORMAT_TEXT) {
            Matcher matcher = BLOB_PATTERN.matcher(ByteBufUtils.decode(byteBuf));

            if (!matcher.find()) {
                throw new IllegalStateException("ByteBuf does not contain BYTEA hex format");
            }
            decoded = ByteBufUtil.decodeHexDump(matcher.group(1));
        } else {
            decoded = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(decoded);
        }
        return decoded;
    }

    ByteBuf toHexFormat(ByteBuf b) {
        int blobSize = b.readableBytes();
        ByteBuf buf = this.byteBufAllocator.buffer(2 + blobSize * 2);
        buf.writeByte('\\').writeByte('x');

        int chunkSize = 1024;

        while (b.isReadable()) {
            chunkSize = Math.min(chunkSize, b.readableBytes());
            buf.writeCharSequence(ByteBufUtil.hexDump(b, b.readerIndex(), chunkSize), StandardCharsets.US_ASCII);
            b.skipBytes(chunkSize);
        }

        return buf;
    }

}
