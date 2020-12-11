/*
 * Copyright 2020 the original author or authors.
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

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

/**
 * Utility methods to decode a numeric value.
 */
final class NumericDecodeUtils {

    private NumericDecodeUtils() {
    }

    /**
     * Decode {@code buffer} to {@link Number} according to {@link PostgresqlObjectId}.
     *
     * @param buffer   the data buffer
     * @param dataType the well-known {@link PostgresqlObjectId type OID}
     * @param format   the data type {@link Format}, text or binary
     * @return the decoded number
     */
    public static Number decodeNumber(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format) {

        Assert.requireNonNull(buffer, "byteBuf must not be null");

        switch (dataType) {

            case NUMERIC:
                if (format == FORMAT_BINARY) {
                    return decodeBinary(buffer);
                }
                return new BigDecimal(ByteBufUtils.decode(buffer));

            case INT2:
                if (FORMAT_BINARY == format) {
                    return buffer.readShort();
                }
                return Short.parseShort(ByteBufUtils.decode(buffer));
            case INT4:
            case OID:
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
            default:
                throw new UnsupportedOperationException(String.format("Cannot decode value for type %s, format %s", dataType, format));
        }
    }

    public static BigDecimal decodeBinary(ByteBuf byteBuf) {

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
