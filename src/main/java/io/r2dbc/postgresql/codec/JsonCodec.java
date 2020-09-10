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
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.io.InputStream;
import java.nio.ByteBuffer;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;

final class JsonCodec extends AbstractJsonCodec<Json> {

    private final ByteBufAllocator byteBufAllocator;

    private final boolean preferAttachedBuffers;

    JsonCodec(ByteBufAllocator byteBufAllocator, boolean preferAttachedBuffers) {
        super(Json.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.preferAttachedBuffers = preferAttachedBuffers;
    }

    @Override
    Json doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends Json> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (dataType == JSONB && format == FORMAT_BINARY) {
            buffer.skipBytes(1);
        }

        if (this.preferAttachedBuffers) {
            return new Json.JsonOutput(buffer.retainedSlice());
        }

        return new Json.JsonByteArrayInput(ByteBufUtil.getBytes(buffer));
    }

    @Override
    Parameter doEncode(Json value) {
        Assert.requireNonNull(value, "value must not be null");
        if (!(value instanceof Json.JsonInput || value instanceof Json.JsonOutput)) {
            throw new IllegalArgumentException("value must be JsonInput or JsonOutput");
        }

        return create(JSONB, FORMAT_BINARY, () -> {

            Object toEncode;

            if (value instanceof Json.JsonInput) {
                toEncode = ((Json.JsonInput<?>) value).value;
            } else {
                toEncode = ((Json.JsonOutput) value).buffer;
                ((Json.JsonOutput) value).released = true;
            }

            if (toEncode instanceof InputStream) {
                return JsonInputStreamCodec.doEncode((InputStream) toEncode, this.byteBufAllocator);
            }

            if (toEncode instanceof String) {
                toEncode = ByteBufUtils.encode(this.byteBufAllocator, (String) toEncode);
            }

            if (toEncode instanceof ByteBuffer) {
                toEncode = Unpooled.wrappedBuffer((ByteBuffer) toEncode);
            }

            if (toEncode instanceof byte[]) {
                toEncode = Unpooled.wrappedBuffer((byte[]) toEncode);
            }

            ByteBuf buffer = (ByteBuf) toEncode;
            buffer.touch();

            try {
                return this.byteBufAllocator.buffer(buffer.readableBytes() + 1).writeByte(1).writeBytes(buffer);
            } finally {
                ReferenceCountUtil.safeRelease(buffer);
            }
        });

    }

}
