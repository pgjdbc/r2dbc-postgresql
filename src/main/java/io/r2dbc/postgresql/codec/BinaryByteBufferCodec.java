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
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;

import java.nio.ByteBuffer;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class BinaryByteBufferCodec extends AbstractBinaryCodec<ByteBuffer> {

    BinaryByteBufferCodec(ByteBufAllocator byteBufAllocator) {
        super(ByteBuffer.class, byteBufAllocator);
    }

    @Override
    ByteBuffer doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends ByteBuffer> type) {
        return ByteBuffer.wrap(decode(format, buffer));
    }

    @Override
    EncodedParameter doEncode(ByteBuffer value, PostgresqlObjectId dataType) {
        return create(FORMAT_TEXT, dataType, () -> encodeToHex(Unpooled.wrappedBuffer(value)));
    }

}
