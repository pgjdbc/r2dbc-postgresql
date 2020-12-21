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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.nio.ByteBuffer;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class JsonByteBufferCodec extends AbstractJsonCodec<ByteBuffer> {

    private final ByteBufAllocator byteBufAllocator;

    JsonByteBufferCodec(ByteBufAllocator byteBufAllocator) {
        super(ByteBuffer.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    ByteBuffer doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends ByteBuffer> type) {

        ByteBuffer result = ByteBuffer.allocate(buffer.readableBytes());
        buffer.readBytes(result);
        result.flip();

        return result;
    }

    @Override
    EncodedParameter doEncode(ByteBuffer value, PostgresqlObjectId dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(FORMAT_TEXT, dataType, () -> this.byteBufAllocator.buffer(value.remaining()).writeBytes(value));
    }

}
