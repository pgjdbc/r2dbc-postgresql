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
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ArrayCodec<Boolean>}.
 */
class BooleanArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Boolean> {

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

    @Override
    ArrayCodec<Boolean> createInstance() {
        return new ArrayCodec<>(TEST, BOOL_ARRAY, new BooleanCodec(TEST), Boolean.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return BOOL;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return BOOL_ARRAY;
    }

    @Override
    ByteBuf getSingleDimensionBinaryArray() {
        return SINGLE_DIM_BINARY_ARRAY;
    }

    @Override
    ByteBuf getTwoDimensionBinaryArray() {
        return TWO_DIM_BINARY_ARRAY;
    }

    @Override
    Class<? extends Boolean[]> getSingleDimensionArrayType() {
        return Boolean[].class;
    }

    @Override
    Class<? extends Boolean[][]> getTwoDimensionArrayType() {
        return Boolean[][].class;
    }

    @Override
    Boolean[] getExpectedSingleDimensionArray() {
        return new Boolean[]{false, false, true, true, false};
    }

    @Override
    Boolean[][] getExpectedTwoDimensionArray() {
        return new Boolean[][]{{true}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{f,F,t,T,f}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{true},{NULL}}";
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

}
