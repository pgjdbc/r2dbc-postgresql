/*
 * Copyright 2017 the original author or authors.
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
import io.r2dbc.postgresql.util.Assert;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;

final class BooleanArrayCodec extends AbstractArrayCodec<Boolean> {

    BooleanArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, Boolean.class, BOOL_ARRAY);
    }

    @Override
    Boolean doDecodeBinary(ByteBuf byteBuffer) {
        return byteBuffer.readBoolean();
    }

    @Override
    Boolean doDecodeText(String text) {
        return "1".equals(text)
            || "true".equalsIgnoreCase(text)
            || "t".equalsIgnoreCase(text)
            || "yes".equalsIgnoreCase(text)
            || "y".equalsIgnoreCase(text)
            || "on".equalsIgnoreCase(text);
    }

    @Override
    String doEncodeText(Boolean value) {
        Assert.requireNonNull(value, "value must not be null");

        return value ? "t" : "f";
    }

}
