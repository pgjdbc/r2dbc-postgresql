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
import io.r2dbc.postgresql.util.Assert;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

abstract class AbstractArrayCodec<T> extends AbstractCodec<Object[]> {

    private static final byte DELIMITER = ',';

    private final ByteBufAllocator byteBufAllocator;

    private final Class<T> componentType;

    AbstractArrayCodec(ByteBufAllocator byteBufAllocator, Class<T> componentType) {
        super(Object[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.componentType = Assert.requireNonNull(componentType, "componentType must not be null");
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return isTypeAssignable(value.getClass());
    }

    abstract T decodeItem(ByteBuf byteBuf, Format format, Class<?> type);

    @Override
    final Object[] doDecode(ByteBuf byteBuf, Format format, Class<? extends Object[]> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireArrayDimension(type, 1, "type must be an array with one dimension");

        List<T> items = new ArrayList<>();

        if (FORMAT_BINARY == format) {
            while (byteBuf.readableBytes() != 0) {
                items.add(decodeItem(byteBuf, format, type));
            }
        } else {
            byteBuf.skipBytes(1);

            for (int size = byteBuf.bytesBefore(DELIMITER); size != -1; size = byteBuf.bytesBefore(DELIMITER)) {
                items.add(decodeItem(byteBuf.readSlice(size), format, type.getComponentType()));
                byteBuf.skipBytes(1); // skip delimiter
            }

            items.add(decodeItem(byteBuf.readSlice(byteBuf.readableBytes() - 1), format, this.componentType));
        }

        Object[] a = (Object[]) Array.newInstance(this.componentType, items.size());
        return items.toArray(a);
    }

    @Override
    @SuppressWarnings("unchecked")
    final Parameter doEncode(Object[] value) {
        Assert.requireNonNull(value, "value must not be null");
        Assert.requireArrayDimension(value.getClass(), 1, "value must be an array with one dimension");

        ByteBuf byteBuf = this.byteBufAllocator.buffer();
        byteBuf.writeByte('{');

        if (value.length > 0) {
            encodeItem(byteBuf, (T) value[0]);

            for (int i = 1; i < value.length; i++) {
                byteBuf.writeByte(DELIMITER);
                encodeItem(byteBuf, (T) value[i]);
            }
        }

        byteBuf.writeByte('}');

        return encodeArray(byteBuf);
    }

    abstract Parameter encodeArray(ByteBuf byteBuf);

    abstract void encodeItem(ByteBuf byteBuf, T value);

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (!type.isArray()) {
            return false;
        }

        return getBaseComponentType(type).equals(this.componentType);
    }

    private static Class<?> getBaseComponentType(Class<?> type) {
        Class<?> t = type;

        while (t.isArray()) {
            t = t.getComponentType();
        }

        return t;
    }
}
