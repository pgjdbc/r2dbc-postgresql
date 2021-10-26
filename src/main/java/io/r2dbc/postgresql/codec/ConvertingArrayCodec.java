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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.util.EnumSet;
import java.util.Set;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.DATE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.OID_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMETZ_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

/**
 * Array codec that is capable of conversion by accepting a range of OID's delegating to a specific target codec.
 *
 * @param <T>
 */
final class ConvertingArrayCodec<T> extends ArrayCodec<T> {

    static final Set<PostgresqlObjectId> NUMERIC_ARRAY_TYPES = EnumSet.of(INT2_ARRAY, INT4_ARRAY, INT8_ARRAY, FLOAT4_ARRAY, FLOAT8_ARRAY, NUMERIC_ARRAY, OID_ARRAY);

    static final Set<PostgresqlObjectId> DATE_ARRAY_TYPES = EnumSet.of(DATE_ARRAY, TIMESTAMP_ARRAY, TIMESTAMPTZ_ARRAY, TIME_ARRAY, TIMETZ_ARRAY);

    private final ArrayCodecDelegate<T> delegate;

    private final Class<T> componentType;

    private final Set<PostgresqlObjectId> supportedTypes;

    public ConvertingArrayCodec(ByteBufAllocator byteBufAllocator, ArrayCodecDelegate<T> delegate, Class<T> componentType, Set<PostgresqlObjectId> supportedTypes) {
        super(byteBufAllocator, delegate, componentType);
        this.delegate = delegate;
        this.componentType = componentType;
        this.supportedTypes = supportedTypes;
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {

        // consider delegate priority
        if (type == Object.class && dataType == getDelegate().getArrayDataType().getObjectId()) {
            return true;
        }

        return PostgresqlObjectId.isValid(dataType) && this.supportedTypes.contains(PostgresqlObjectId.valueOf(dataType)) &&
            type.isArray() && getActualComponentType(type).isAssignableFrom(getComponentType());
    }

    @Override
    Object[] doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends Object[]> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (FORMAT_BINARY == format) {
            return decodeBinary(buffer, dataType, this.delegate, this.componentType, type);
        } else {
            return decodeText(buffer, dataType, ArrayCodec.COMMA, this.delegate, this.componentType, type);
        }
    }

}
