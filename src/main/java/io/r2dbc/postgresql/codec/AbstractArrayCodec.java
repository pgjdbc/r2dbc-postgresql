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

import java.util.Objects;
import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
abstract class AbstractArrayCodec<T> extends AbstractCodec<T[]> {

    private final ByteBufAllocator byteBufAllocator;

    AbstractArrayCodec(ByteBufAllocator byteBufAllocator, Class<T[]> type) {
        super(type);
        this.byteBufAllocator = Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    @SuppressWarnings("unchecked")
    T[] doDecode(ByteBuf byteBuf, Format format, Class<? extends T[]> type) {
        Objects.requireNonNull(byteBuf, "byteBuf must not be null");

        Stream.Builder<T> builder = Stream.builder();
        byteBuf.skipBytes(1);
        for (int size = byteBuf.bytesBefore((byte) ','); size != -1; size = byteBuf.bytesBefore((byte) ',')) {
            T item = decodeItem(byteBuf.readSlice(size), format, type.getComponentType());
            builder.accept(item);
            byteBuf.skipBytes(1); // skip delimiter
        }
        T item = decodeItem(byteBuf.readSlice(byteBuf.readableBytes() - 1), format, type.getComponentType());
        builder.accept(item);

        return (T[]) builder.build().toArray();
    }

    @Override
    Parameter doEncode(T[] value) {
        Objects.requireNonNull(value, "value must not be null");

        ByteBuf byteBuf = byteBufAllocator.buffer();
        byteBuf.writeByte('{');

        if (value.length > 0) {
            encodeItem(byteBuf, value[0]);
            for (int i = 1; i < value.length; i++) {
                byteBuf.writeByte(',');
                encodeItem(byteBuf, value[i]);
            }
        }
        byteBuf.writeByte('}');

        return encodeArray(byteBuf);
    }

    abstract T decodeItem(ByteBuf byteBuf, Format format, Class<?> type);

    abstract void encodeItem(ByteBuf byteBuf, T value);

    abstract Parameter encodeArray(ByteBuf byteBuf);
}
