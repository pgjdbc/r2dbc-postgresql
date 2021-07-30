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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.PATH;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.PATH_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Path>}.
 */
final class PathArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Path> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(602) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(1 + 4 + 16 * /* number of points */3) // number of bytes to follow
        .writeByte(1) // closed path
        .writeInt(3) // number of points
        .writeDouble(1.1) // point 1 x
        .writeDouble(2.2) // point 1 y
        .writeDouble(10.10) // point 2 x
        .writeDouble(10.10) // point 2 y
        .writeDouble(1.1) // point 3 x
        .writeDouble(2.2) // point 3 y
        .writeInt(1 + 4 + 16 * /* number of points */4) // number of bytes to follow
        .writeByte(0) // open path
        .writeInt(4) // number of points
        .writeDouble(1.1) // point 1 x
        .writeDouble(2.2) // point 1 y
        .writeDouble(10.10) // point 2 x
        .writeDouble(10.10) // point 2 y
        .writeDouble(0.42) // point 3 x
        .writeDouble(5.3) // point 3 y
        .writeDouble(-3.5) // point 4 x
        .writeDouble(0.0); // point 4 y

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(602) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(1 + 4 + 16 * /* number of points */2) // number of bytes to follow
        .writeByte(1) // closed path
        .writeInt(2) // number of points
        .writeDouble(-10.42) // point 1 x
        .writeDouble(3.14) // point 1 y
        .writeDouble(10.42) // point 2 x
        .writeDouble(-3.14) // point 2 y
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Path> createInstance() {
        return new ArrayCodec<>(TEST, PATH_ARRAY, new PathCodec(TEST), Path.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return PATH;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return PATH_ARRAY;
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
    Class<? extends Path[]> getSingleDimensionArrayType() {
        return Path[].class;
    }

    @Override
    Class<? extends Path[][]> getTwoDimensionArrayType() {
        return Path[][].class;
    }

    @Override
    Path[] getExpectedSingleDimensionArray() {
        return new Path[]{Path.of(false, Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(1.1, 2.2)), Path.of(true, Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3),
            Point.of(-3.5, 0.0))};
    }

    @Override
    Path[][] getExpectedTwoDimensionArray() {
        return new Path[][]{{Path.of(false, Point.of(-10.42, 3.14), Point.of(10.42, -3.14))}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"((1.1, 2.2),(10.10, 10.10),(1.1, 2.2))\",\"[(1.1,2.2),(10.10,10.10),(0.42,5.3),(-3.5,0.0)]\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"((-10.42,3.14),(10.42,-3.14))\"},{NULL}}";
    }

}
