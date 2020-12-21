/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.time.OffsetTime;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMETZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link OffsetTimeCodec}.
 */
class OffsetTimeCodecUnitTests {

    private static final int dataType = TIMETZ.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OffsetTimeCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        OffsetTime offsetTime = OffsetTime.parse("21:16:41.123456-09:00");

        assertThat(new OffsetTimeCodec(TEST).decode(encode(TEST, "21:16:41.123456-09:00"), dataType, FORMAT_TEXT, OffsetTime.class))
            .isEqualTo(offsetTime);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new OffsetTimeCodec(TEST).decode(null, dataType, FORMAT_TEXT, OffsetTime.class)).isNull();
    }

    @Test
    void doCanDecode() {
        OffsetTimeCodec codec = new OffsetTimeCodec(TEST);

        assertThat(codec.doCanDecode(TIMETZ, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIMETZ, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OffsetTimeCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OffsetTimeCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        OffsetTime offsetTime = OffsetTime.now();

        ParameterAssert.assertThat(new OffsetTimeCodec(TEST).doEncode(offsetTime))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIMETZ.getObjectId())
            .hasValue(encode(TEST, offsetTime.toString()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OffsetTimeCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new OffsetTimeCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, TIMETZ.getObjectId(), NULL_VALUE));
    }

}
