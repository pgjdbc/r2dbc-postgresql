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

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class InstantCodecTest {

    private static final int dataType = TIMESTAMP.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Instant instant = Instant.parse("2018-11-04T11:57:56.159600Z");

        assertThat(new InstantCodec(TEST).decode(encode(TEST, "2018-11-04 11:57:56.159600"), dataType, FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
    }

    @Test
    void decodeFromTimestampWithTimezone() {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2018-11-05T00:20:25.039883+09:00[Asia/Tokyo]");
        Instant instant = zonedDateTime.toInstant();

        assertThat(new InstantCodec(TEST).decode(encode(TEST, "2018-11-05 00:20:25.039883+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new InstantCodec(TEST).decode(null, dataType, FORMAT_TEXT, Instant.class)).isNull();
    }

    @Test
    void doCanDecode() {
        InstantCodec codec = new InstantCodec(TEST);

        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TIMESTAMPTZ, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TIMESTAMPTZ, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Instant instant = Instant.now();

        assertThat(new InstantCodec(TEST).doEncode(instant))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIMESTAMPTZ.getObjectId())
            .hasValue(encode(TEST, instant.toString()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new InstantCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMPTZ.getObjectId(), NULL_VALUE));
    }

}
