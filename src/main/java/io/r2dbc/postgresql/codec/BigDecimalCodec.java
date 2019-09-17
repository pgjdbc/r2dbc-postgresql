/*
 * Copyright 2017-2019 the original author or authors.
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
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NUMERIC;

final class BigDecimalCodec extends AbstractCodec<BigDecimal> {

    private final ByteBufAllocator byteBufAllocator;

    BigDecimalCodec(ByteBufAllocator byteBufAllocator) {
        super(BigDecimal.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(NUMERIC, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return NUMERIC == type;
    }

    @Override
    BigDecimal doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends BigDecimal> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        if (format == FORMAT_BINARY) {
            return doDecodeBinary(buffer);
        }

        return new BigDecimal(ByteBufUtils.decode(buffer));
    }

    @Override
    Parameter doEncode(BigDecimal value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuf encoded = ByteBufUtils.encode(this.byteBufAllocator, value.toString());
        return create(NUMERIC, FORMAT_TEXT, Flux.just(encoded));
    }

    private BigDecimal doDecodeBinary(ByteBuf byteBuf) {
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
