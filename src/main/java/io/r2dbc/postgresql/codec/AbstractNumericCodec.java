/*
 * Copyright 2019-2020 the original author or authors.
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
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.OID;

/**
 * Codec to decode all known numeric types.
 *
 * @param <T> the type that is handled by this {@link Codec}.
 */
abstract class AbstractNumericCodec<T extends Number> extends AbstractCodec<T> {

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, OID);

    /**
     * Create a new {@link AbstractCodec}.
     *
     * @param type the type handled by this codec
     */
    AbstractNumericCodec(Class<T> type) {
        super(type);
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
    T decodeNumber(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, Class<T> expectedType, Function<Number, T> converter) {
        Number number = NumericDecodeUtils.decodeNumber(buffer, dataType, format);
        return potentiallyConvert(number, expectedType, converter);
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(getDefaultType(), FORMAT_BINARY);
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
