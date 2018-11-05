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
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class ByteCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new ByteCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        ByteCodec codec = new ByteCodec(TEST);

        assertThat(codec.decode(TEST.buffer(2).writeShort((byte) 100), FORMAT_BINARY, Byte.class)).isEqualTo((byte) 100);
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "100"), FORMAT_TEXT, Byte.class)).isEqualTo((byte) 100);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new ByteCodec(TEST).decode(null, FORMAT_BINARY, Byte.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new ByteCodec(TEST).decode(TEST.buffer(0), null, Byte.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        ByteCodec codec = new ByteCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_BINARY, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT2)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT2)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new ByteCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new ByteCodec(TEST).doEncode((byte) 100))
            .isEqualTo(new Parameter(FORMAT_BINARY, INT2.getObjectId(), TEST.buffer(2).writeShort(100)));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new ByteCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ByteCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, INT2.getObjectId(), null));
    }

}
