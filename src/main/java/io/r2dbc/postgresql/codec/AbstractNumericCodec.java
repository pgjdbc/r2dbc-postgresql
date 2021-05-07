/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.OID;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Codec to decode all known numeric types.
 *
 * @param <T> the type that is handled by this {@link Codec}.
 */
abstract class AbstractNumericCodec<T extends Number> extends AbstractCodec<T> implements ArrayCodecDelegate<T> {

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, OID);

    private final ByteBufAllocator byteBufAllocator;

    AbstractNumericCodec(Class<T> type, ByteBufAllocator byteBufAllocator) {
        super(type);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (type == Object.class) {
            if (PostgresqlObjectId.isValid(dataType) && PostgresqlObjectId.valueOf(dataType) != getDefaultType()) {
                return false;
            }
        }
        return super.canDecode(dataType, format, type);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");
        return SUPPORTED_TYPES.contains(type);
    }

    @Override
    EncodedParameter doEncode(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return doEncode(value, getDefaultType());
    }

    EncodedParameter doEncode(T value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        if (dataType == NUMERIC) {
            return create(FORMAT_TEXT, dataType, () -> ByteBufUtils.encode(this.byteBufAllocator, value.toString()));
        }

        return create(FORMAT_BINARY, dataType, () -> doEncodeNumber(value, dataType));
    }

    @Override
    public String encodeToText(T value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.toString();
    }

    private ByteBuf doEncodeNumber(Number value, PostgresTypeIdentifier identifier) {

        PostgresqlObjectId oid = PostgresqlObjectId.valueOf(identifier.getObjectId());

        switch (oid) {

            case FLOAT8:
                return this.byteBufAllocator.buffer(8).writeDouble(value.doubleValue());

            case FLOAT4:
                return this.byteBufAllocator.buffer(4).writeFloat(value.floatValue());

            case INT8:
                return this.byteBufAllocator.buffer(8).writeLong(value.longValue());

            case INT4:
                return this.byteBufAllocator.buffer(4).writeInt(value.intValue());

            case INT2:
                return this.byteBufAllocator.buffer(4).writeShort(value.shortValue());
        }

        throw new IllegalArgumentException(String.format("Cannot encode %s to %s", value, oid));

    }

    /**
     * Decode {@code buffer} to {@link Number} and potentially convert it to {@link Class expectedType} using {@link Function converter} if the decoded type does not match {@code expectedType}.
     *
     * @param buffer       the data buffer
     * @param dataType     the well-known {@link PostgresqlObjectId type OID}
     * @param format       the data type {@link Format}, text or binary
     * @param expectedType the expected result type
     * @param converter    the converter function to convert from {@link Number} to {@code expectedType}
     * @return the decoded number
     */
    T decodeNumber(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, Class<T> expectedType, Function<Number, T> converter) {
        Number number = NumericDecodeUtils.decodeNumber(buffer, PostgresqlObjectId.from(dataType), format);
        return potentiallyConvert(number, expectedType, converter);
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(FORMAT_BINARY, getDefaultType());
    }

    /**
     * Returns the {@link PostgresqlObjectId} for to identify whether this codec is the default codec.
     *
     * @return the {@link PostgresqlObjectId} for to identify whether this codec is the default codec.
     */
    abstract PostgresqlObjectId getDefaultType();

    private static <T> T potentiallyConvert(Number number, Class<T> expectedType, Function<Number, T> converter) {
        return expectedType.isInstance(number) ? expectedType.cast(number) : converter.apply(number);
    }

}
