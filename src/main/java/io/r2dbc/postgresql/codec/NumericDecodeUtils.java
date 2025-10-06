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
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

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
            case NUMERIC_ARRAY:
                if (format == FORMAT_BINARY) {
                    return decodeBinary(buffer);
                }
                return new BigDecimal(ByteBufUtils.decode(buffer));
            case INT2:
            case INT2_ARRAY:
                if (FORMAT_BINARY == format) {
                    return buffer.readShort();
                }
                return Short.parseShort(ByteBufUtils.decode(buffer));
            case INT4:
            case INT4_ARRAY:
                if (FORMAT_BINARY == format) {
                    return buffer.readInt();
                }
                return Integer.parseInt(ByteBufUtils.decode(buffer));
            case OID:
            case OID_ARRAY:
                if (FORMAT_BINARY == format) {
                    return buffer.readUnsignedInt();
                }
                return Long.parseLong(ByteBufUtils.decode(buffer));
            case INT8:
            case INT8_ARRAY:
                if (FORMAT_BINARY == format) {
                    return buffer.readLong();
                }
                return Long.parseLong(ByteBufUtils.decode(buffer));
            case FLOAT4:
            case FLOAT4_ARRAY:
                if (FORMAT_BINARY == format) {
                    return buffer.readFloat();
                }
                return Float.parseFloat(ByteBufUtils.decode(buffer));
            case FLOAT8:
            case FLOAT8_ARRAY:
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

        StringBuilder sb = new StringBuilder((sign != 0 ? 1 : 0) + 2 + (digits.length * 4));
        if (sign != 0) {
            sb.append("-");
        }
        sb.append("0.");
        for (short digit : digits) {
            String rendered = "" + digit;
            int padded = rendered.length();

            while (padded < 4) {
                sb.append("0");
                padded++;
            }

            sb.append(rendered);
        }

        return new BigDecimal(sb.toString()).movePointRight((weight + 1) * 4).setScale(scale, RoundingMode.DOWN);
    }

}
