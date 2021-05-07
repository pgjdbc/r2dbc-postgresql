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
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ArrayCodec <Boolean>}.
 */
class BooleanArrayCodecUnitTest {

    private static final int dataType = BOOL_ARRAY.getObjectId();

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(16) // oid
        .writeInt(5) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(1) // length of element
        .writeBoolean(false) // value
        .writeInt(1) // length of element
        .writeBoolean(false) // value
        .writeInt(1) // length of element
        .writeBoolean(true) // value
        .writeInt(1) // length of element
        .writeBoolean(true) // value
        .writeInt(1) // length of element
        .writeBoolean(false); // value

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(16) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(1) // length of element
        .writeBoolean(true) // value
        .writeInt(-1); // length of null element

    private ArrayCodec<Boolean> codec;

    @BeforeEach
    void setup() {
        codec = new ArrayCodec<>(TEST, BOOL_ARRAY, new BooleanCodec(TEST), Boolean.class);
    }

    @Test
    void decodeItem() {
        assertThat(codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Boolean[].class))
            .isEqualTo(new Boolean[]{false, false, true, true, false});
    }

    @Test
    void decodeItem_textArray() {
        Boolean[] expected = {false, false, true, true, false};
        assertThat(codec.decode(encode(TEST, "{f,F,t,T,f}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{false,FALSE,TRUE,true,false}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{0,0,1,1,0}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{no,NO,YES,yes,no}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{n,N,Y,y,n}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{off,OFF,ON,on,off}"), dataType, FORMAT_TEXT, Boolean[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeItem_emptyArray() {
        assertThat(codec.decode(encode(TEST, "{}"), dataType, FORMAT_TEXT, Boolean[][].class))
            .isEqualTo(new Boolean[][]{});
    }

    @Test
    void decodeItem_emptyBinaryArray() {
        ByteBuf buf = TEST
            .buffer()
            .writeInt(0)
            .writeInt(0)
            .writeInt(16);

        assertThat(codec.decode(buf, dataType, FORMAT_BINARY, Boolean[][].class))
            .isEqualTo(new Boolean[][]{});
    }

    @Test
    void decodeItem_expectedLessDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{{f}}"), dataType, FORMAT_TEXT, Boolean[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Boolean[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{f,t}"), dataType, FORMAT_TEXT, Boolean[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Boolean[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_twoDimensionalArrayWithNull() {
        assertThat(codec.decode(encode(TEST, "{{f},{NULL}}"), dataType, FORMAT_TEXT, Boolean[][].class))
            .isEqualTo(new Boolean[][]{{false}, {null}});
    }

    @Test
    void decodeItem_twoDimensionalBinaryArrayWithNull() {
        assertThat(codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Boolean[][].class))
            .isEqualTo(new Boolean[][]{{true}, {null}});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec genericCodec = codec;
        assertThat(genericCodec.canDecode(BOOL_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isTrue();
        Boolean[] expected = {false, false, true, true, false};

        assertThat(genericCodec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Object.class))
            .isEqualTo(expected);
        assertThat(genericCodec.decode(encode(TEST, "{f,f,t,t,f}"), dataType, FORMAT_TEXT, Object.class))
            .isEqualTo(expected);
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(BOOL, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(BOOL_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(BOOL_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(codec.encodeArray(() -> encode(TEST, "{f,t}"), BOOL_ARRAY))
            .hasFormat(FORMAT_TEXT)
            .hasType(BOOL_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{f,t}"));
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, BOOL_ARRAY.getObjectId(), NULL_VALUE));
    }

}
