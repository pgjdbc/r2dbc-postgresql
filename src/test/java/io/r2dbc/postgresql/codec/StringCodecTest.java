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
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class StringCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new StringCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        assertThat(new StringCodec(TEST).decode(encode(TEST, "test"), TEXT, String.class))
            .isEqualTo("test");
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new StringCodec(TEST).decode(null, TEXT, String.class)).isNull();
    }

    @Test
    void doCanDecode() {
        StringCodec codec = new StringCodec(TEST);

        assertThat(codec.doCanDecode(BINARY, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(TEXT, MONEY)).isFalse();
        assertThat(codec.doCanDecode(TEXT, CHAR)).isTrue();
        assertThat(codec.doCanDecode(TEXT, PostgresqlObjectId.TEXT)).isTrue();
        assertThat(codec.doCanDecode(TEXT, UNKNOWN)).isTrue();
        assertThat(codec.doCanDecode(TEXT, VARCHAR)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new StringCodec(TEST).doCanDecode(null, VARCHAR))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new StringCodec(TEST).doCanDecode(TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        String string = "test";

        assertThat(new StringCodec(TEST).doEncode(string))
            .isEqualTo(new Parameter(TEXT, VARCHAR.getObjectId(), encode(TEST, "test")));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new StringCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new StringCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(TEXT, VARCHAR.getObjectId(), null));
    }

}
