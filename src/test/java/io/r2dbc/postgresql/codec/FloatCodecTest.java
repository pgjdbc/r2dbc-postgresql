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

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class FloatCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new FloatCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        FloatCodec codec = new FloatCodec(TEST);

        assertThat(codec.decode(TEST.buffer(4).writeFloat(100.0f), BINARY, Float.class)).isEqualTo(100.0f);
        assertThat(codec.decode(encode(TEST, "100.0"), TEXT, Float.class)).isEqualTo(100.0f);
    }

    @Test
    void decodeNoByteBuf() {
        assertThatNullPointerException().isThrownBy(() -> new FloatCodec(TEST).decode(null, BINARY, Float.class))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void decodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new FloatCodec(TEST).decode(TEST.buffer(0), null, Float.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        FloatCodec codec = new FloatCodec(TEST);

        assertThat(codec.doCanDecode(BINARY, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(BINARY, FLOAT4)).isTrue();
        assertThat(codec.doCanDecode(TEXT, FLOAT4)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new FloatCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new FloatCodec(TEST).doEncode(100f))
            .isEqualTo(new Parameter(BINARY, FLOAT4.getObjectId(), TEST.buffer(4).writeFloat(100)));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new FloatCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

}
