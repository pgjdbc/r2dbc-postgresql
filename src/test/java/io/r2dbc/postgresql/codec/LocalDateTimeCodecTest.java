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

import java.time.LocalDateTime;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class LocalDateTimeCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new LocalDateTimeCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        LocalDateTime localDateTime = LocalDateTime.parse("2018-11-05T00:06:31.700426");

        assertThat(new LocalDateTimeCodec(TEST).decode(encode(TEST, "2018-11-05 00:06:31.700426"), FORMAT_TEXT, LocalDateTime.class))
            .isEqualTo(localDateTime);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LocalDateTimeCodec(TEST).decode(null, FORMAT_TEXT, LocalDateTime.class)).isNull();
    }

    @Test
    void doCanDecode() {
        LocalDateTimeCodec codec = new LocalDateTimeCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_BINARY, TIMESTAMP)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, MONEY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, TIMESTAMP)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doCanDecode(null, VARCHAR))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doCanDecode(FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        LocalDateTime localDateTime = LocalDateTime.now();

        assertThat(new LocalDateTimeCodec(TEST).doEncode(localDateTime))
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMP.getObjectId(), encode(TEST, localDateTime.toString())));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new LocalDateTimeCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LocalDateTimeCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TIMESTAMP.getObjectId(), null));
    }

}
