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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Float>}.
 */
class FloatArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Float> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(700) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(4) // length of element
        .writeFloat(100.5f) // value
        .writeInt(4) // length of element
        .writeFloat(200.25f); // value

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(700) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(4) // length of element
        .writeFloat(100.5f) // value
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Float> createInstance() {
        return new ArrayCodec<>(TEST, new FloatCodec(TEST), Float.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return FLOAT4;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return FLOAT4_ARRAY;
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
    Class<? extends Float[]> getSingleDimensionArrayType() {
        return Float[].class;
    }

    @Override
    Class<? extends Float[][]> getTwoDimensionArrayType() {
        return Float[][].class;
    }

    @Override
    Float[] getExpectedSingleDimensionArray() {
        return new Float[]{100.5f, 200.25f};
    }

    @Override
    Float[][] getExpectedTwoDimensionArray() {
        return new Float[][]{{100.5f}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{100.5,200.25}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{100.5},{NULL}}";
    }

}
