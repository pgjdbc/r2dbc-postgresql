/*
 * Copyright 2021 the original author or authors.
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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.util.function.Function;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Base class for typical built-in codecs that support to-text encoding for singular values and array items through {@link ArrayCodecDelegate}.
 */
abstract class BuiltinCodecSupport<T> extends AbstractCodec<T> implements ArrayCodecDelegate<T> {

    private final ByteBufAllocator byteBufAllocator;

    private final PostgresqlObjectId postgresType;

    private final PostgresqlObjectId postgresArrayType;

    private final Function<T, String> toTextEncoder;

    /**
     * Create a new {@link BuiltinCodecSupport}.
     *
     * @param type              the type handled by this codec.
     * @param byteBufAllocator  allocator
     * @param postgresType      Postgres OID for singular values
     * @param postgresArrayType Postgres array type OID variant of {@code postgresType}
     * @param toTextEncoder     function to encode a value to {@link String}
     */
    BuiltinCodecSupport(Class<T> type, ByteBufAllocator byteBufAllocator, PostgresqlObjectId postgresType, PostgresqlObjectId postgresArrayType, Function<T, String> toTextEncoder) {
        super(type);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.postgresType = Assert.requireNonNull(postgresType, "postgresType must not be null");
        this.postgresArrayType = Assert.requireNonNull(postgresArrayType, "postgresArrayType must not be null");
        this.toTextEncoder = Assert.requireNonNull(toTextEncoder, "toTextEncoder must not be null");
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return this.postgresType == type;
    }

    @Override
    final EncodedParameter doEncode(T value) {
        return doEncode(value, this.postgresType);
    }

    @Override
    final EncodedParameter doEncode(T value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(FORMAT_TEXT, dataType, () -> ByteBufUtils.encode(this.byteBufAllocator, encodeToText(value)));
    }

    @Override
    public final String encodeToText(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.toTextEncoder.apply(value);
    }

    @Override
    public final EncodedParameter encodeNull() {
        return createNull(FORMAT_TEXT, this.postgresType);
    }

    @Override
    public final PostgresTypeIdentifier getArrayDataType() {
        return this.postgresArrayType;
    }

}
