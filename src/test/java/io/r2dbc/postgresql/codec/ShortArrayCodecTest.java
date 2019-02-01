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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ShortArrayCodecTest {

    @Test
    void decodeItem() {
        ShortArrayCodec codec = new ShortArrayCodec(TEST);

        assertThat(codec.decode(TEST.buffer(4).writeShort(100).writeShort(200), FORMAT_BINARY, Short[].class)).isEqualTo(new short[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Short[].class)).isEqualTo(new short[]{100, 200});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec codec = new ShortArrayCodec(TEST);

        assertThat(codec.decode(TEST.buffer(4).writeShort(100).writeShort(200), FORMAT_BINARY, Object.class)).isEqualTo(new short[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Object.class)).isEqualTo(new short[]{100, 200});
    }

    @Test
    void decodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).decodeItem(null, FORMAT_TEXT, null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void decodeItemNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).decodeItem(TEST.buffer(0), null, null))
            .withMessage("format must not be null");
    }

    @Test
    void decodeMultidimensional() {
        ShortArrayCodec codec = new ShortArrayCodec(TEST);

        assertThatIllegalArgumentException().isThrownBy(() -> codec.decode(ByteBufUtils.encode(TEST, "{{100},{200}}"), FORMAT_TEXT, Integer[][].class))
            .withMessage("type must be an array with one dimension");
    }

    @Test
    void doCanDecode() {
        ShortArrayCodec codec = new ShortArrayCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_TEXT, INT2)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT2_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT2_ARRAY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new ShortArrayCodec(TEST).encodeArray(ByteBufUtils.encode(TEST, "{100,200}")))
            .isEqualTo(new Parameter(FORMAT_TEXT, INT2_ARRAY.getObjectId(), ByteBufUtils.encode(TEST, "{100,200}")));
    }

    @Test
    void encodeArrayNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).encodeArray(null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItem() {
        ByteBuf actual = TEST.buffer(3);

        new ShortArrayCodec(TEST).encodeItem(actual, (short) 100);

        assertThat(actual).isEqualTo(ByteBufUtils.encode(TEST, "100"));
    }

    @Test
    void encodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).encodeItem(null, (short) 100))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortArrayCodec(TEST).encodeItem(TEST.buffer(0), null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ShortArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, INT2_ARRAY.getObjectId(), null));
    }
}