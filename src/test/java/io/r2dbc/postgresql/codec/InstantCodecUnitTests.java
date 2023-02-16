/*
 * Copyright 2017 the original author or authors.
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link InstantCodec}.
 */
final class InstantCodecUnitTests {

    private static final int dataType = TIMESTAMP.getObjectId();

    private final InstantCodec codec = new InstantCodec(TEST, ZoneId::systemDefault);

    @Test
    void cannotDecodeCustomType() {
        assertThat(this.codec.canDecode(72093, FORMAT_TEXT, Object.class)).isFalse();
    }

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InstantCodec(null, null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Instant instant = LocalDateTime.parse("2018-11-04T11:57:56.159600").atZone(ZoneId.systemDefault()).toInstant();

        assertThat(this.codec.decode(encode(TEST, "2018-11-04 11:57:56.159600"), dataType, FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
    }

    @Test
    void decodeFromTimestampWithTimezone() {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2018-11-05T00:20:25.039883+09:00[Asia/Tokyo]");
        Instant instant = zonedDateTime.toInstant();

        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+09:00:00"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
                .isEqualTo(instant);
        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+09:00"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
    }

    @Test
    void decodeFromTimestampWithTimezoneUTC() {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2018-11-05T00:20:25.039883+00:00");
        Instant instant = zonedDateTime.toInstant();

        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+00:00:00"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
                .isEqualTo(instant);
        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+00:00"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
        assertThat(this.codec.decode(encode(TEST, "2018-11-05 00:20:25.039883+00"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Instant.class))
            .isEqualTo(instant);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(this.codec.decode(null, dataType, FORMAT_TEXT, Instant.class)).isNull();
    }

    @Test
    void doCanDecode() {
        InstantCodec codec = this.codec;

        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TIMESTAMPTZ, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TIMESTAMPTZ, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Instant instant = Instant.parse("2018-11-05T00:20:25.039883Z");

        assertThat(this.codec.doEncode(instant))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIMESTAMPTZ.getObjectId())
            .hasValue(encode(TEST, "2018-11-05 00:20:25.039883+00"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(this.codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, TIMESTAMPTZ.getObjectId(), NULL_VALUE));
    }

}
