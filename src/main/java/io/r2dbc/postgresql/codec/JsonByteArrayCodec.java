/*
 * Copyright 2017-2020 the original author or authors.
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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;

final class JsonByteArrayCodec extends AbstractJsonCodec<byte[]> {

    private final ByteBufAllocator byteBufAllocator;

    JsonByteArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byte[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    byte[] doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends byte[]> type) {

        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

    @Override
    EncodedParameter doEncode(byte[] value, PostgresqlObjectId dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(JSON, FORMAT_TEXT, () -> this.byteBufAllocator.buffer(value.length).writeBytes(value));
    }

}
