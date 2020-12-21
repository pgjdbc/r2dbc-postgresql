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
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LocalTimeCodec}.
 */
final class LocalTimeCodecUnitTests {

    private static final int dataType = TIME.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalTimeCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        LocalTime localTime = LocalTime.now();

        assertThat(new LocalTimeCodec(TEST).decode(encode(TEST, localTime.toString()), dataType, FORMAT_TEXT, LocalTime.class))
            .isEqualTo(localTime);
    }

    @Test
    void decodeFromTimestamp() {
        LocalDateTime localDateTime = LocalDateTime.parse("2018-11-05T00:06:31.700426");
        LocalTime localTime = localDateTime.toLocalTime();

        assertThat(new LocalTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:06:31.700426"), TIMESTAMP.getObjectId(), FORMAT_TEXT, LocalTime.class))
            .isEqualTo(localTime);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LocalTimeCodec(TEST).decode(null, dataType, FORMAT_TEXT, LocalTime.class)).isNull();
    }

    @Test
    void doCanDecode() {
        LocalTimeCodec codec = new LocalTimeCodec(TEST);

        assertThat(codec.doCanDecode(TIME, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIME, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalTimeCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalTimeCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        LocalTime localTime = LocalTime.now();

        assertThat(new LocalTimeCodec(TEST).doEncode(localTime))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIME.getObjectId())
            .hasValue(encode(TEST, localTime.toString()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalTimeCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalTimeCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, TIME.getObjectId(), NULL_VALUE));
    }

}
