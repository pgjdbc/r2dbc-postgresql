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
import io.r2dbc.postgresql.type.PostgresqlObjectId;
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

    private final IntegerArrayCodec codec = new IntegerArrayCodec(TEST);

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
            .buffer()
            .writeInt(1)
            .writeInt(0)
            .writeInt(23)
            .writeInt(2)
            .writeInt(1)
            .writeInt(4)
            .writeInt(100)
            .writeInt(4)
            .writeInt(200);

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
            .buffer()
            .writeInt(2) // num of dims
            .writeInt(1) // flag: has nulls
            .writeInt(23) // oid
            .writeInt(2) // dim 1 length
            .writeInt(1) // dim 1 lower bound
            .writeInt(1) // dim 2 length
            .writeInt(1) // dim 2 lower bound
            .writeInt(4) // length of element
            .writeInt(100) // value
            .writeInt(-1); // length of null element

    @Test
    void decodeItem() {
        assertThat(codec.decode(SINGLE_DIM_BINARY_ARRAY, FORMAT_BINARY, Integer[].class)).isEqualTo(new int[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Integer[].class))
                .isEqualTo(new int[]{100, 200});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec codec = new IntegerArrayCodec(TEST);
        codec.canDecode(PostgresqlObjectId.INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class);

        assertThat(codec.decode(SINGLE_DIM_BINARY_ARRAY, FORMAT_BINARY, Object.class)).isEqualTo(new int[]{100, 200});
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Object.class)).isEqualTo(new int[]{100, 200});
    }

    @Test
    void decodeItem_emptyArray() {
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{}"), FORMAT_TEXT, Integer[][].class))
                .isEqualTo(new int[][]{});
    }

    @Test
    void decodeItem_emptyBinaryArray() {
        ByteBuf buf = TEST
                .buffer()
                .writeInt(0)
                .writeInt(0)
                .writeInt(23);

        assertThat(codec.decode(buf, FORMAT_BINARY, Integer[][].class)).isEqualTo(new int[][]{});
    }

    @Test
    void decodeItem_twoDimensionalArrayWithNull() {
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "{{100},{NULL}}"), FORMAT_TEXT, Integer[][].class))
                .isEqualTo(new Integer[][]{{100}, {null}});
    }

    @Test
    void decodeItem_twoDimensionalBinaryArrayWithNull() {
        assertThat(codec.decode(TWO_DIM_BINARY_ARRAY, FORMAT_BINARY, Integer[][].class)).isEqualTo(new Integer[][]{{100}, {null}});
    }

    @Test
    void decodeItem_expectedMoreDimensionsInArray() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> codec.decode(ByteBufUtils.encode(TEST, "{100,200}"), FORMAT_TEXT, Integer[][].class))
                .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> codec.decode(SINGLE_DIM_BINARY_ARRAY, FORMAT_BINARY, Integer[][].class))
                .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInArray() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> codec.decode(ByteBufUtils.encode(TEST, "{{100}}"), FORMAT_TEXT, Integer[].class))
                .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> codec.decode(TWO_DIM_BINARY_ARRAY, FORMAT_BINARY, Integer[].class))
                .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT4)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT4_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT4_ARRAY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(codec.encodeArray(ByteBufUtils.encode(TEST, "{100,200}")))
            .isEqualTo(new Parameter(FORMAT_TEXT, INT4_ARRAY.getObjectId(), ByteBufUtils.encode(TEST, "{100,200}")));
    }

    @Test
    void encodeItem() {
        assertThat(codec.encodeItem(100)).isEqualTo("100");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encodeItem(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, INT4_ARRAY.getObjectId(), null));
    }
}