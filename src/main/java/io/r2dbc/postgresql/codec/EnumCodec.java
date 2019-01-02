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
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

@SuppressWarnings("rawtypes")
final class EnumCodec implements Codec<Enum> {

    private final StringCodec delegate;

    EnumCodec(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.delegate = new StringCodec(byteBufAllocator);
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        return Enum.class.isAssignableFrom(type) && this.delegate.doCanDecode(format, PostgresqlObjectId.valueOf(dataType));
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");
        return Enum.class.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");
        return Enum.class.isAssignableFrom(type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Enum decode(ByteBuf byteBuf, Format format, Class<? extends Enum> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        if (byteBuf == null) {
            return null;
        }
        return Enum.valueOf(type, this.delegate.doDecode(byteBuf, format, String.class));
    }

    @Override
    public Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");
        return this.delegate.encode(((Enum) value).name());
    }

    @Override
    public Parameter encodeNull() {
        return this.delegate.encodeNull();
    }
}
