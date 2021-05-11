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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Integer>}.
 */
final class IntegerArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Integer> {

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

    @Override
    ArrayCodec<Integer> createInstance() {
        return new ArrayCodec<>(TEST, new IntegerCodec(TEST), Integer.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return INT4;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return INT4_ARRAY;
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
    Class<? extends Integer[]> getSingleDimensionArrayType() {
        return Integer[].class;
    }

    @Override
    Class<? extends Integer[][]> getTwoDimensionArrayType() {
        return Integer[][].class;
    }

    @Override
    Integer[] getExpectedSingleDimensionArray() {
        return new Integer[]{100, 200};
    }

    @Override
    Integer[][] getExpectedTwoDimensionArray() {
        return new Integer[][]{{100}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{100,200}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{100},{NULL}}";
    }

}
