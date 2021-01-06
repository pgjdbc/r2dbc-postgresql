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
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;

final class JsonByteBufCodec extends AbstractJsonCodec<ByteBuf> {

    private final ByteBufAllocator byteBufAllocator;

    JsonByteBufCodec(ByteBufAllocator byteBufAllocator) {
        super(ByteBuf.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    ByteBuf doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends ByteBuf> type) {
        return buffer.readRetainedSlice(buffer.readableBytes()).touch("Decoded by JsonByteBufCodec");
    }

    @Override
    Parameter doEncode(ByteBuf value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(JSONB, FORMAT_BINARY, () -> {
            try {
                return this.byteBufAllocator.buffer(value.readByte() + 1).writeByte(1).writeBytes(value);
            } finally {
                ReferenceCountUtil.release(value);
            }
        });
    }

}
