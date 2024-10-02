/*
 * Copyright 2024 the original author or authors.
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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

/**
 * @since 1.0.6
 */
final class PrimitiveCodec<T> implements Codec<Object> {

    private final Class<?> primitiveType;

    private final Class<T> wrapperType;

    private final Codec<T> boxedCodec;

    public PrimitiveCodec(Class<?> primitiveType, Class<T> wrapperType, Codec<T> boxedCodec) {

        this.primitiveType = Assert.requireNonNull(primitiveType, "primitiveType must not be null");
        this.wrapperType = Assert.requireNonNull(wrapperType, "wrapperType must not be null");
        this.boxedCodec = Assert.requireNonNull(boxedCodec, "boxedCodec must not be null");

        Assert.isTrue(primitiveType.isPrimitive(), "primitiveType must be a primitive type");
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        return this.primitiveType.equals(type) && this.boxedCodec.canDecode(dataType, format, this.wrapperType);
    }

    @Override
    public boolean canEncode(Object value) {
        return this.boxedCodec.canEncode(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        return this.primitiveType.equals(type) && this.boxedCodec.canEncodeNull(this.wrapperType);
    }

    @Override
    public Object decode(ByteBuf buffer, int dataType, Format format, Class<?> type) {

        T value = this.boxedCodec.decode(buffer, dataType, format, this.wrapperType);

        if (value == null) {
            throw new NullPointerException("value for primitive type " + this.primitiveType.getName() + " is null");
        }

        return value;
    }

    @Override
    public EncodedParameter encode(Object value) {
        return this.boxedCodec.encode(value);
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {
        return this.boxedCodec.encode(value, dataType);
    }

    @Override
    public EncodedParameter encodeNull() {
        return this.boxedCodec.encodeNull();
    }
}
