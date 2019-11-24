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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.OID;;

/**
 * Codec to decode all known numeric types.
 *
 * @param <T>
 */
abstract class AbstractNumericCodec<T extends Number> extends AbstractCodec<T> {

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC, OID);

    /**
     * Creates a new {@link AbstractCodec}.
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
            if (PostgresqlObjectId.valueOf(dataType) != getDefaultType()) {
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
     * @param buffer
     * @param dataType
     * @param format
     * @param expectedType
     * @param converter
     * @return
     */
    T decodeNumber(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, Class<T> expectedType, Function<Number, T> converter) {
        Number number = decodeNumber(buffer, dataType, format);
        return potentiallyConvert(number, expectedType, converter);
    }

    /**
     * Decode {@code buffer} to {@link Number} according to {@link PostgresqlObjectId}.
     *
     * @param buffer
     * @param dataType
     * @param format
     * @return
     */
    private Number decodeNumber(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        switch (dataType) {

            case NUMERIC:
                if (format == FORMAT_BINARY) {
                    return decodeBinaryBigDecimal(buffer);
                }
                return new BigDecimal(ByteBufUtils.decode(buffer));

            case INT2:
                if (FORMAT_BINARY == format) {
                    return buffer.readShort();
                }
                return Short.parseShort(ByteBufUtils.decode(buffer));
            case INT4:
                if (FORMAT_BINARY == format) {
                    return buffer.readInt();
                }
                return Integer.parseInt(ByteBufUtils.decode(buffer));
            case INT8:
                if (FORMAT_BINARY == format) {
                    return buffer.readLong();
                }
                return Long.parseLong(ByteBufUtils.decode(buffer));
            case FLOAT4:
                if (FORMAT_BINARY == format) {
                    return buffer.readFloat();
                }
                return Float.parseFloat(ByteBufUtils.decode(buffer));
            case FLOAT8:
                if (FORMAT_BINARY == format) {
                    return buffer.readDouble();
                }
                return Double.parseDouble(ByteBufUtils.decode(buffer));
            case OID:
                if (FORMAT_BINARY == format) {
                    return buffer.readInt();
                }
                return Integer.parseInt(ByteBufUtils.decode(buffer));
            default:
                throw new UnsupportedOperationException(String.format("Cannot decode value for type %s, format %s", dataType, format));
        }
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

    private static BigDecimal decodeBinaryBigDecimal(ByteBuf byteBuf) {
        // extract values
        short numOfDigits = byteBuf.readShort();
        if (numOfDigits == 0) {
            return BigDecimal.ZERO;
        }
        short weight = byteBuf.readShort();
        short sign = byteBuf.readShort();
        short scale = byteBuf.readShort();
        short[] digits = new short[numOfDigits];
        for (short i = 0; i < numOfDigits; i++) {
            digits[i] = byteBuf.readShort();
        }

        StringBuilder builder = new StringBuilder();
        // whole part
        builder.append(digits[0]);
        for (short i = 0; i < weight * 4; i++) {
            builder.append(0);
        }
        // decimal part
        if (scale > 0) {
            builder.append('.');
            for (short i = 0; i < scale; i++) {
                builder.append(0);
            }
        }

        int expectedLength = builder.length();
        int baseOffset = Short.toString(digits[0]).length();

        for (short i = 1; i < numOfDigits; i++) {
            weight--;
            String temp = Short.toString(digits[i]);
            int offset = baseOffset + 4 * i - temp.length();
            if (weight < 0) {
                offset++; // dot between whole and decimal parts
            }
            builder.replace(offset, offset + temp.length(), temp);
        }

        builder.setLength(expectedLength); // remove zeros from the end

        if (sign == 0) {
            return new BigDecimal(builder.toString());
        } else {
            return new BigDecimal("-" + builder.toString());
        }
    }
}
