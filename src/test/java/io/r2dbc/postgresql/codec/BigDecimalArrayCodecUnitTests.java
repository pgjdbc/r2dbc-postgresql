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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ArrayCodec <BigDecimal>}.
 */
final class BigDecimalArrayCodecUnitTests {

    private static final int dataType = NUMERIC_ARRAY.getObjectId();

    private final ByteBuf BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1)
        .writeInt(0)
        .writeInt(NUMERIC.getObjectId())
        .writeInt(2)
        .writeInt(0)
        .writeInt(10)
        .writeShort(1)
        .writeShort(0)
        .writeShort(0)
        .writeShort(2)
        .writeShort(100)
        .writeInt(10)
        .writeShort(1)
        .writeShort(0)
        .writeShort(0)
        .writeShort(2)
        .writeShort(200);

    ArrayCodec<BigDecimal> codec;

    @BeforeEach
    void setup() {
        codec = new ArrayCodec<>(TEST, new BigDecimalCodec(TEST), BigDecimal.class);
    }

    @Test
    void decode() {
        assertThat(codec.decode(BINARY_ARRAY, dataType, FORMAT_BINARY, BigDecimal[].class))
            .isEqualTo(new BigDecimal[]{new BigDecimal("100.00"), new BigDecimal("200.00")});

        assertThat(codec.decode(encode(TEST, "{100.00,200.00}"), dataType, FORMAT_TEXT, BigDecimal[].class))
            .isEqualTo(new BigDecimal[]{new BigDecimal("100.00"), new BigDecimal("200.00")});
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(codec.decode(null, dataType, FORMAT_TEXT, BigDecimal[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(NUMERIC_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        BigDecimal[] value = {
            new BigDecimal("100.00"),
            new BigDecimal("200.00")
        };

        ParameterAssert.assertThat(codec.doEncode(value))
            .hasFormat(FORMAT_TEXT)
            .hasType(NUMERIC_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{100.00,200.00}"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, NUMERIC_ARRAY.getObjectId(), NULL_VALUE));
    }

}
