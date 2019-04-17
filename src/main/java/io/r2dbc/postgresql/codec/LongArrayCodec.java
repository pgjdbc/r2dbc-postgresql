/*
 * Copyright 2017-2019 the original author or authors.
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
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8_ARRAY;

final class LongArrayCodec extends AbstractArrayCodec<Long> {

    LongArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, Long.class);
    }

    @Override
    public Long decodeItem(ByteBuf byteBuf, Format format, @Nullable Class<?> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");

        if (FORMAT_BINARY == format) {
            return byteBuf.readLong();
        } else {
            return Long.parseLong(ByteBufUtils.decode(byteBuf));
        }
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FORMAT_TEXT, INT8_ARRAY);
    }

    @Override
    boolean doCanDecode(Format format, PostgresqlObjectId type) {
        Assert.requireNonNull(type, "type must not be null");

        return INT8_ARRAY == type;
    }

    @Override
    Parameter encodeArray(ByteBuf byteBuf) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");

        return create(FORMAT_TEXT, INT8_ARRAY, Flux.just(byteBuf));
    }

    @Override
    void encodeItem(ByteBuf byteBuf, Long value) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(value, "value must not be null");

        ByteBufUtil.writeUtf8(byteBuf, value.toString());
    }

}
