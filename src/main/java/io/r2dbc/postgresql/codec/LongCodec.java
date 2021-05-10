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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;

final class LongCodec extends AbstractNumericCodec<Long> {


    LongCodec(ByteBufAllocator byteBufAllocator) {
        super(Long.class, byteBufAllocator);
    }

    @Override
    Long doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, @Nullable Class<? extends Long> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");

        return decodeNumber(buffer, dataType, format, Long.class, Number::longValue);
    }

    @Override
    PostgresqlObjectId getDefaultType() {
        return INT8;
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return INT8_ARRAY;
    }

}
