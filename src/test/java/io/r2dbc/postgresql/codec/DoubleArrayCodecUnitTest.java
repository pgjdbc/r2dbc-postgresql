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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ArrayCodec <Double>}.
 */
class DoubleArrayCodecUnitTest {

    private static final int dataType = FLOAT8_ARRAY.getObjectId();

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(701) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(8) // length of element
        .writeDouble(100.5) // value
        .writeInt(8) // length of element
        .writeDouble(200.25); // value

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(701) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(8) // length of element
        .writeDouble(100.5) // value
        .writeInt(-1); // length of null element

    ArrayCodec<Double> codec;

    @BeforeEach
    void setup() {
        codec = new ArrayCodec<>(TEST, new DoubleCodec(TEST), Double.class);
    }

    @Test
    void decodeItem() {
        Double[] expected = {100.5, 200.25};
        assertThat(codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Double[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{100.5,200.25}"), dataType, FORMAT_TEXT, Double[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeItem_emptyArray() {
        assertThat(codec.decode(encode(TEST, "{}"), dataType, FORMAT_TEXT, Double[][].class))
            .isEqualTo(new Double[][]{});
    }

    @Test
    void decodeItem_emptyBinaryArray() {
        ByteBuf buf = TEST
            .buffer()
            .writeInt(0)
            .writeInt(0)
            .writeInt(701);

        assertThat(codec.decode(buf, dataType, FORMAT_BINARY, Double[][].class))
            .isEqualTo(new Double[][]{});
    }

    @Test
    void decodeItem_expectedLessDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{{100.5}}"), dataType, FORMAT_TEXT, Double[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Double[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{100.5,200.25}"), dataType, FORMAT_TEXT, Double[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Float[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_twoDimensionalArrayWithNull() {
        assertThat(codec.decode(encode(TEST, "{{100.5},{NULL}}"), dataType, FORMAT_TEXT, Double[][].class))
            .isEqualTo(new Double[][]{{100.5}, {null}});
    }

    @Test
    void decodeItem_twoDimensionalBinaryArrayWithNull() {
        assertThat(codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Double[][].class))
            .isEqualTo(new Double[][]{{100.5}, {null}});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec genericCodec = codec;
        genericCodec.canDecode(dataType, FORMAT_TEXT, Object.class);

        assertThat(genericCodec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Object.class)).isEqualTo(new Double[]{100.5, 200.25});
        assertThat(genericCodec.decode(encode(TEST, "{100.5,200.25}"), dataType, FORMAT_TEXT, Object.class)).isEqualTo(new Double[]{100.5, 200.25});
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(FLOAT8, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(FLOAT8_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(FLOAT8_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(codec.encodeArray(() -> encode(TEST, "{100.5,200.25}"), FLOAT8_ARRAY))
            .hasFormat(FORMAT_TEXT)
            .hasType(FLOAT8_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{100.5,200.25}"));
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
