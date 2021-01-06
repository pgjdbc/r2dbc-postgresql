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
import java.util.Date;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.DATE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link DateCodec}.
 */
final class DateCodecUnitTests {

    private static final int dataType = TIMESTAMP.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DateCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Instant testInstant = LocalDateTime.parse("2010-02-01T10:08:04.412").atZone(ZoneId.systemDefault()).toInstant();
        Date date = Date.from(testInstant);

        assertThat(new DateCodec(TEST).decode(encode(TEST, "2010-02-01 10:08:04.412"), dataType, FORMAT_TEXT, Date.class))
            .isEqualTo(date);
    }

    @Test
    void decodeFromDate() {
        Date date = Date.from(LocalDateTime.parse("2018-11-04T00:00:00.000").atZone(ZoneId.systemDefault()).toInstant());

        assertThat(new DateCodec(TEST).decode(encode(TEST, "2018-11-04"), DATE.getObjectId(), FORMAT_TEXT, Date.class))
            .isEqualTo(date);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new DateCodec(TEST).decode(null, dataType, FORMAT_TEXT, Date.class)).isNull();
    }

    @Test
    void doCanDecode() {
        DateCodec codec = new DateCodec(TEST);

        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TIMESTAMP, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DateCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DateCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Instant testInstant = LocalDateTime.parse("2010-02-01T10:08:04.412").atZone(ZoneId.systemDefault()).toInstant();
        Date date = Date.from(testInstant);

        assertThat(new DateCodec(TEST).doEncode(date))
            .hasFormat(FORMAT_TEXT)
            .hasType(TIMESTAMP.getObjectId())
            .hasValue(encode(TEST, "2010-02-01T10:08:04.412"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DateCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new DateCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, TIMESTAMP.getObjectId(), NULL_VALUE));
    }

}
