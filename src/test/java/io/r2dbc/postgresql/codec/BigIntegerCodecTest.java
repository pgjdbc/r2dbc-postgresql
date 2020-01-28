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

import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class BigIntegerCodecTest {

    private static final int dataType = NUMERIC.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BigIntegerCodec(null))
                .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        BigInteger bigInteger = new BigInteger("100");

        assertThat(new BigIntegerCodec(TEST).decode(encode(TEST, bigInteger.toString()), dataType, FORMAT_TEXT, BigInteger.class))
                .isEqualTo(bigInteger);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BigIntegerCodec(TEST).decode(null, dataType, FORMAT_TEXT, BigInteger.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BigIntegerCodec codec = new BigIntegerCodec(TEST);

        assertThat(codec.doCanDecode(NUMERIC, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(NUMERIC, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BigIntegerCodec(TEST).doCanDecode(VARCHAR, null))
                .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BigIntegerCodec(TEST).doCanDecode(null, FORMAT_TEXT))
                .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        BigInteger bigInteger = new BigInteger("100");

        assertThat(new BigIntegerCodec(TEST).doEncode(bigInteger))
                .hasFormat(FORMAT_TEXT)
                .hasType(NUMERIC.getObjectId())
                .hasValue(encode(TEST, "100"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BigIntegerCodec(TEST).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BigIntegerCodec(TEST).encodeNull())
                .isEqualTo(new Parameter(FORMAT_TEXT, NUMERIC.getObjectId(), NULL_VALUE));
    }
    
}
