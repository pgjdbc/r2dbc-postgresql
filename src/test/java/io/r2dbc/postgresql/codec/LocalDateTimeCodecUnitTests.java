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

import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LocalDateTimeCodec}.
 */
final class LocalDateTimeCodecUnitTests {

    private static final int dataType = TIMESTAMP.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        LocalDateTime localDateTime = LocalDateTime.parse("2018-11-05T00:06:31.700426");

        assertThat(new LocalDateTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:06:31.700426"), dataType, FORMAT_TEXT, LocalDateTime.class))
            .isEqualTo(localDateTime);
    }

    @Test
    void decodeUTC() {
        LocalDateTime localDateTime = LocalDateTime.parse("2018-11-05T00:06:31.700426");

        assertThat(new LocalDateTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:06:31.700426+00:00"), dataType, FORMAT_TEXT, LocalDateTime.class))
                .isEqualTo(localDateTime);
        assertThat(new LocalDateTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:06:31.700426+00"), dataType, FORMAT_TEXT, LocalDateTime.class))
                .isEqualTo(localDateTime);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LocalDateTimeCodec(TEST).decode(null, dataType, FORMAT_TEXT, LocalDateTime.class)).isNull();
    }

    @Test
    void doCanDecode() {
        LocalDateTimeCodec codec = new LocalDateTimeCodec(TEST);

        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        LocalDateTime localDateTime = LocalDateTime.now();

        assertThat(new LocalDateTimeCodec(TEST).doEncode(localDateTime))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIMESTAMP.getObjectId())
            .hasValue(encode(TEST, localDateTime.toString()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalDateTimeCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMP.getObjectId(), NULL_VALUE));
    }

}
