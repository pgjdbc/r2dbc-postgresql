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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;

final class IntegerCodec extends AbstractNumericCodec<Integer> implements PrimitiveWrapperCodecProvider<Integer> {

    IntegerCodec(ByteBufAllocator byteBufAllocator) {
        super(Integer.class, byteBufAllocator);
    }

    @Override
    public PrimitiveCodec<Integer> getPrimitiveCodec() {
        return new PrimitiveCodec<>(Integer.TYPE, Integer.class, this);
    }

    @Override
    Integer doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends Integer> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");

        return decodeNumber(buffer, dataType, format, Integer.class, Number::intValue);
    }

    @Override
    PostgresqlObjectId getDefaultType() {
        return INT4;
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return INT4_ARRAY;
    }

}
