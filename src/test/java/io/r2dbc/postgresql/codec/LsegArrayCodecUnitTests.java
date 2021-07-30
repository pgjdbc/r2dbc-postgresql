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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LSEG;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LSEG_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Lseg>}.
 */
final class LsegArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Lseg> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(601) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(32) // length of element
        .writeDouble(1.11) // the x axis coordinate of point p1
        .writeDouble(2.22) // the y axis coordinate of point p1
        .writeDouble(3.33) // the x axis coordinate of point p2
        .writeDouble(4.44) // the y axis coordinate of point p2
        .writeInt(32) // length of element
        .writeDouble(6.6) // the x axis coordinate of point p1
        .writeDouble(3.5) // the y axis coordinate of point p1
        .writeDouble(6.6) // the x axis coordinate of point p2
        .writeDouble(-2.36); // the y axis coordinate of point p2

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(601) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(32) // length of element
        .writeDouble(1.11) // the x axis coordinate of point p1
        .writeDouble(2.22) // the y axis coordinate of point p1
        .writeDouble(3.33) // the x axis coordinate of point p2
        .writeDouble(4.44) // the y axis coordinate of point p2
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Lseg> createInstance() {
        return new ArrayCodec<>(TEST, LSEG_ARRAY, new LsegCodec(TEST), Lseg.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return LSEG;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return LSEG_ARRAY;
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
    Class<? extends Lseg[]> getSingleDimensionArrayType() {
        return Lseg[].class;
    }

    @Override
    Class<? extends Lseg[][]> getTwoDimensionArrayType() {
        return Lseg[][].class;
    }

    @Override
    Lseg[] getExpectedSingleDimensionArray() {
        return new Lseg[]{Lseg.of(1.11, 2.22, 3.33, 4.44), Lseg.of(6.6, 3.5, 6.6, -2.36)};
    }

    @Override
    Lseg[][] getExpectedTwoDimensionArray() {
        return new Lseg[][]{{Lseg.of(1.11, 2.22, 3.33, 4.44)}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"[ ( 1.11, 2.22 ) , ( 3.33, 4.44 ) ]\",\"[ ( 6.6, 3.5 ) , ( 6.6, -2.36 ) ]\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"[ ( 1.11, 2.22 ) , ( 3.33, 4.44 ) ]\"},{NULL}}";
    }

}
