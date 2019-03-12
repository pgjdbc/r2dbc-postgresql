/*
 * Copyright 2017-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LongArrayCodecTest {

    private final ByteBuf BINARY_ARRAY = TEST
            .buffer()
            .writeInt(1)
            .writeInt(0)
            .writeInt(20)
            .writeInt(2)
            .writeInt(2)
            .writeInt(8)
            .writeLong(100L)
            .writeInt(8)
            .writeLong(200L);

    @Test
    void decodeItem() {
        LongArrayCodec codec = new LongArrayCodec(TEST);

        assertThat(codec.decode(BINARY_ARRAY, FORMAT_BINARY, Long[].class)).isEqualTo(new long[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Long[].class)).isEqualTo(new long[]{100, 200});
    }

    @Test
    void doCanDecode() {
        LongArrayCodec codec = new LongArrayCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_TEXT, INT8)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT8_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT8_ARRAY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongArrayCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new LongArrayCodec(TEST).encodeArray(ByteBufUtils.encode(TEST, "{100,200}")))
            .isEqualTo(new Parameter(FORMAT_TEXT, INT8_ARRAY.getObjectId(), ByteBufUtils.encode(TEST, "{100,200}")));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec codec = new LongArrayCodec(TEST);

        assertThat(codec.decode(BINARY_ARRAY, FORMAT_BINARY, Object.class)).isEqualTo(new long[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Object.class)).isEqualTo(new long[]{100, 200});
    }

    @Test
    void encodeArrayNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongArrayCodec(TEST).encodeArray(null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItem() {
        assertThat(new LongArrayCodec(TEST).encodeItem(100L)).isEqualTo("100");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongArrayCodec(TEST).encodeItem(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LongArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, INT8_ARRAY.getObjectId(), null));
    }

}