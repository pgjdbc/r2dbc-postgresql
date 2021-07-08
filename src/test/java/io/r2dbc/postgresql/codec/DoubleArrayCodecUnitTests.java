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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ArrayCodec<Double>}.
 */
class DoubleArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Double> {

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

    @Override
    ArrayCodec<Double> createInstance() {
        return new ConvertingArrayCodec<>(TEST, new DoubleCodec(TEST), Double.class, ConvertingArrayCodec.NUMERIC_ARRAY_TYPES);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return FLOAT8;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return FLOAT8_ARRAY;
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
    Class<? extends Double[]> getSingleDimensionArrayType() {
        return Double[].class;
    }

    @Override
    Class<? extends Double[][]> getTwoDimensionArrayType() {
        return Double[][].class;
    }

    @Override
    Double[] getExpectedSingleDimensionArray() {
        return new Double[]{100.5, 200.25};
    }

    @Override
    Double[][] getExpectedTwoDimensionArray() {
        return new Double[][]{{100.5}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{100.5,200.25}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{100.5},{NULL}}";
    }

    @Test
    void decodeItemFromFloat4Array() {
        Double[] expected = new Double[]{100.5, 200.25, 300.125};
        byte[] data = ByteBufUtil.decodeHexDump("0000000100000000000002bc00000003000000010000000442c9000000000004434840000000000443961000");
        assertThat(codec.decode(Unpooled.wrappedBuffer(data), FLOAT4_ARRAY, FORMAT_BINARY, Double[].class))
            .isEqualTo(expected);
    }

}
