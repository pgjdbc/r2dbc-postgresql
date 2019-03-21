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
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;

final class ShortCodec extends AbstractCodec<Short> {

    private final ByteBufAllocator byteBufAllocator;

    ShortCodec(ByteBufAllocator byteBufAllocator) {
        super(Short.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FORMAT_BINARY, INT2);
    }

    @Override
    boolean doCanDecode(@Nullable Format format, PostgresqlObjectId type) {
        Assert.requireNonNull(type, "type must not be null");

        return INT2 == type;
    }

    @Override
    Short doDecode(ByteBuf byteBuf, Format format, @Nullable Class<? extends Short> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");

        if (FORMAT_BINARY == format) {
            return byteBuf.readShort();
        } else {
            return Short.parseShort(ByteBufUtils.decode(byteBuf));
        }
    }

    @Override
    Parameter doEncode(Short value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuf encoded = this.byteBufAllocator.buffer(2).writeShort(value);
        return create(FORMAT_BINARY, INT2, encoded);
    }

}
