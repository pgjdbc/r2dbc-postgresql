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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class IntegerArrayCodecTest {

    @Test
    void decodeItem() {
        IntegerArrayCodec codec = new IntegerArrayCodec(TEST);

        assertThat(codec.decode(TEST.buffer(8).writeInt(100).writeInt(200), FORMAT_BINARY, Integer[].class)).isEqualTo(new int[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Integer[].class)).isEqualTo(new int[]{100, 200});
    }

    @Test
    void decodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).decodeItem(null, FORMAT_TEXT, null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void decodeItemNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).decodeItem(TEST.buffer(0), null, null))
            .withMessage("format must not be null");
    }

    @Test
    void decodeMultidimensional() {
        IntegerArrayCodec codec = new IntegerArrayCodec(TEST);

        assertThatIllegalArgumentException().isThrownBy(() -> codec.decode(ByteBufUtils.encode(TEST, "{{100},{200}}"), FORMAT_TEXT, Integer[][].class))
            .withMessage("type must be an array with one dimension");
    }

    @Test
    void doCanDecode() {
        IntegerArrayCodec codec = new IntegerArrayCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_TEXT, INT4)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT4_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT4_ARRAY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new IntegerArrayCodec(TEST).encodeArray(ByteBufUtils.encode(TEST, "{100,200}")))
            .isEqualTo(new Parameter(FORMAT_TEXT, INT4_ARRAY.getObjectId(), ByteBufUtils.encode(TEST, "{100,200}")));
    }

    @Test
    void encodeArrayNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).encodeArray(null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItem() {
        ByteBuf actual = TEST.buffer(3);

        new IntegerArrayCodec(TEST).encodeItem(actual, 100);

        assertThat(actual).isEqualTo(ByteBufUtils.encode(TEST, "100"));
    }

    @Test
    void encodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).encodeItem(null, 100))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerArrayCodec(TEST).encodeItem(TEST.buffer(0), null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new IntegerArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, INT4_ARRAY.getObjectId(), null));
    }
}