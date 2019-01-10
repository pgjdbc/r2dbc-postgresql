/*
 * Copyright 2017-2019 the original author or authors.
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
        this.componentType = componentType;
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    abstract T decodeItem(ByteBuf byteBuf, Format format, Class<?> type);

    boolean isTypeAssignable(Class<?> type) {
        if (!type.isArray()) {
            return false;
        }

        return getBaseComponentType(type).equals(componentType);
    }

    @Override
    public boolean canEncode(Object value) {
        return isTypeAssignable(value.getClass());
    }

    @Override
    final Object[] doDecode(ByteBuf byteBuf, Format format, Class<? extends Object[]> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        assertArrayDimension(type, 1);

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

            items.add(decodeItem(byteBuf.readSlice(byteBuf.readableBytes() - 1), format, componentType));
        }

        Object[] a = (Object[]) Array.newInstance(componentType, items.size());
        return items.toArray(a);
    }

    @Override
    @SuppressWarnings("unchecked")
    final Parameter doEncode(Object[] value) {
        Assert.requireNonNull(value, "value must not be null");
        assertArrayDimension(value.getClass(), 1);

        ByteBuf byteBuf = byteBufAllocator.buffer();
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

    private static Class<?> getBaseComponentType(Class<?> type) {
        if (!type.isArray()) {
            return type;
        }

        Class<?> currentType = type;
        while (currentType.isArray()) {
            currentType = currentType.getComponentType();
        }

        return currentType;
    }

    private static int getDimensions(Class<?> type) {
        if (!type.isArray()) {
            return 0;
        }

        int dimensions = 0;
        Class<?> currentType = type;
        while (currentType.isArray()) {
            currentType = currentType.getComponentType();
            dimensions++;
        }

        return dimensions;
    }

    private static void assertArrayDimension(Class<?> type, int allowed) {
        int actual = getDimensions(type);
        if (actual != allowed) {
            throw new IllegalArgumentException(String.format(
                    "Arrays dimension mismatch. Given %d; expected %d", actual, allowed
            ));
        }
    }
}
