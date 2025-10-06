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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.util.function.Function;

/**
 * @since 1.0.8
 */
class IntegerCodecDelegate<T> extends AbstractCodec<T> {

    private final IntegerCodec delegate;
    private final Function<T, Integer> toIntegerConverter;
    private final Function<Integer, T> fromIntegerConverter;

    IntegerCodecDelegate(Class<T> type, ByteBufAllocator byteBufAllocator, Function<T,Integer> toIntegerConverter, Function<Integer, T> fromIntegerConverter) {
        super(type);
        this.delegate = new IntegerCodec(byteBufAllocator);
        this.toIntegerConverter = toIntegerConverter;
        this.fromIntegerConverter = fromIntegerConverter;
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        return this.delegate.doCanDecode(type, format);
    }

    @Override
    T doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends T> type) {
        final Integer number = this.delegate.doDecode(buffer, dataType, format, Integer.TYPE);
        return this.fromIntegerConverter.apply(number);
    }

    @Override
    EncodedParameter doEncode(T value) {
        Assert.requireNonNull(value, "value must not be null");
        return this.delegate.doEncode(this.toIntegerConverter.apply(value));
    }

    @Override
    EncodedParameter doEncode(T value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");
        Assert.requireNonNull(dataType, "dataType must not be null");
        return this.delegate.doEncode(this.toIntegerConverter.apply(value), dataType);
    }

    @Override
    public Iterable<? extends PostgresTypeIdentifier> getDataTypes() {
        return this.delegate.getDataTypes();
    }

    @Override
    public EncodedParameter encodeNull() {
        return this.delegate.encodeNull();
    }

}
