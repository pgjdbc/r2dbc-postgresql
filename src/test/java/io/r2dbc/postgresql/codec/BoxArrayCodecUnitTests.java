/*
 * Copyright 2021 the original author or authors.
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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Box>}.
 */
final class BoxArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Box> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(603) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(32) // length of element
        .writeDouble(1.9) // x axis coordinate of point a
        .writeDouble(2.8) // y axis coordinate of point a
        .writeDouble(3.7) // x axis coordinate of point b
        .writeDouble(4.6) // y axis coordinate of point b
        .writeInt(32) // length of element
        .writeDouble(5) // x axis coordinate of point a
        .writeDouble(7) // y axis coordinate of point a
        .writeDouble(1.5) // x axis coordinate of point b
        .writeDouble(3.3); // y axis coordinate of point b

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(603) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(32) // length of element
        .writeDouble(1.2) // x axis coordinate of point a
        .writeDouble(123.1) // y axis coordinate of point a
        .writeDouble(-1.2) // x axis coordinate of point b
        .writeDouble(-123.1) // y axis coordinate of point b
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Box> createInstance() {
        return new ArrayCodec<>(TEST, BOX_ARRAY, new BoxCodec(TEST), Box.class, (byte) ';');
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return BOX;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return BOX_ARRAY;
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
    Class<? extends Box[]> getSingleDimensionArrayType() {
        return Box[].class;
    }

    @Override
    Class<? extends Box[][]> getTwoDimensionArrayType() {
        return Box[][].class;
    }

    @Override
    Box[] getExpectedSingleDimensionArray() {
        return new Box[]{Box.of(1.9, 2.8, 3.7, 4.6), Box.of(5, 7, 1.5, 3.3)};
    }

    @Override
    Box[][] getExpectedTwoDimensionArray() {
        return new Box[][]{{Box.of(1.2, 123.1, -1.2, -123.1)}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{((1.9, 2.8),(3.7, 4.6));(5,7),(1.5,3.3)}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{((1.2, 123.1),(-1.2, -123.1))},{NULL}}";
    }

}
