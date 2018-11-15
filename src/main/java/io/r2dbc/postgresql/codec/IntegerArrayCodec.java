/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.util.Objects;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.*;

final class IntegerArrayCodec extends AbstractArrayCodec<Integer> {

    IntegerArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, Integer[].class);
    }

    @Override
    boolean doCanDecode(Format format, PostgresqlObjectId type) {
        return INT4_ARRAY == type;
    }

    @Override
    public Integer decodeItem(ByteBuf byteBuf, @Nullable Format format, @Nullable Class<?> type) {
        Objects.requireNonNull(byteBuf, "byteBuf must not be null");
        Objects.requireNonNull(format, "format must not be null");

        if (FORMAT_BINARY == format) {
            return byteBuf.readInt();
        } else {
            return Integer.parseInt(ByteBufUtils.decode(byteBuf));
        }
    }

    @Override
    void encodeItem(ByteBuf byteBuf, Integer value) {
        Objects.requireNonNull(value, "value must not be null");

        ByteBufUtil.writeUtf8(byteBuf, value.toString());
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FORMAT_TEXT, INT4_ARRAY);
    }

    @Override
    Parameter encodeArray(ByteBuf byteBuf) {
        return create(FORMAT_TEXT, INT4_ARRAY, byteBuf);
    }
}
