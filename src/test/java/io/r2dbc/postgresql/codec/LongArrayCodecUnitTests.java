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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Long>}.
 */
final class LongArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Long> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1)
        .writeInt(0)
        .writeInt(20)
        .writeInt(2)
        .writeInt(2)
        .writeInt(8)
        .writeLong(100L)
        .writeInt(8)
        .writeLong(200L);

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(20) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(8) // length of element
        .writeLong(100L) // value
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Long> createInstance() {
        return new ArrayCodec<>(TEST, new LongCodec(TEST), Long.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return INT8;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return INT8_ARRAY;
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
    Class<? extends Long[]> getSingleDimensionArrayType() {
        return Long[].class;
    }

    @Override
    Class<? extends Long[][]> getTwoDimensionArrayType() {
        return Long[][].class;
    }

    @Override
    Long[] getExpectedSingleDimensionArray() {
        return new Long[]{100L, 200L};
    }

    @Override
    Long[][] getExpectedTwoDimensionArray() {
        return new Long[][]{{100L}, {null}};
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
