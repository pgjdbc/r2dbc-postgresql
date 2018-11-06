/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.time.ZonedDateTime;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class ZonedDateTimeCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new ZonedDateTimeCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2018-11-05T00:20:25.039883+09:00[Asia/Tokyo]");

        assertThat(new ZonedDateTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:20:25.039883+09"), FORMAT_TEXT, ZonedDateTime.class))
            .isEqualTo(zonedDateTime);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new ZonedDateTimeCodec(TEST).decode(null, FORMAT_TEXT, ZonedDateTime.class)).isNull();
    }

    @Test
    void doCanDecode() {
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_BINARY, TIMESTAMPTZ)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, MONEY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, TIMESTAMPTZ)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new ZonedDateTimeCodec(TEST).doCanDecode(null, VARCHAR))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new ZonedDateTimeCodec(TEST).doCanDecode(FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();

        assertThat(new ZonedDateTimeCodec(TEST).doEncode(zonedDateTime))
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMPTZ.getObjectId(), encode(TEST, zonedDateTime.toOffsetDateTime().toString())));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new ZonedDateTimeCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ZonedDateTimeCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMPTZ.getObjectId(), null));
    }

}
