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
import reactor.util.annotation.Nullable;

import java.util.function.Function;

/**
 * Delegate utility for types using {@link StringCodec}.
 *
 * @since 0.8.8
 */
class StringCodecDelegate<T> extends AbstractCodec<T> implements ArrayCodecDelegate<T> {

    private final StringCodec delegate;

    private final Function<T, String> toTextEncoder;

    private final Function<String, T> fromTextDecoder;

    StringCodecDelegate(Class<T> type, ByteBufAllocator byteBufAllocator, Function<T, String> toTextEncoder, Function<String, T> fromTextDecoder) {
        super(type);

        this.delegate = new StringCodec(byteBufAllocator);

        this.toTextEncoder = Assert.requireNonNull(toTextEncoder, "toTextEncoder must not be null");
        this.fromTextDecoder = Assert.requireNonNull(fromTextDecoder, "fromTextDecoder must not be null");
    }

    @Override
    final public EncodedParameter encodeNull() {
        return this.delegate.encodeNull();
    }

    @Override
    final boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.delegate.doCanDecode(type, format);
    }

    @Override
    final T doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends T> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return this.fromTextDecoder.apply(this.delegate.doDecode(buffer, dataType, format, String.class).trim());
    }

    @Override
    final EncodedParameter doEncode(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(encodeToText(value));
    }

    @Override
    final EncodedParameter doEncode(T value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(encodeToText(value), dataType);
    }

    @Override
    public final String encodeToText(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.toTextEncoder.apply(value);
    }

    @Override
    public final PostgresTypeIdentifier getArrayDataType() {
        return PostgresqlObjectId.VARCHAR_ARRAY;
    }

    @Override
    public Iterable<Format> getFormats() {
        return this.delegate.getFormats();
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return this.delegate.getDataTypes();
    }

}
