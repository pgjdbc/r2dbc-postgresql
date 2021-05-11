package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Line>}.
 */
final class LineArrayCodecUnitTest extends AbstractArrayCodecUnitTests<Line> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(628) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(24) // length of element
        .writeDouble(-10.42) // A
        .writeDouble(3.14) // B
        .writeDouble(5.24) // C
        .writeInt(24) // length of element
        .writeDouble(5.5) // A
        .writeDouble(3.2) // B
        .writeDouble(8); // C

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(628) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(24) // length of element
        .writeDouble(-10.42) // A
        .writeDouble(3.14) // B
        .writeDouble(5.24) // C
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Line> createInstance() {
        return new ArrayCodec<>(TEST, LINE_ARRAY, new LineCodec(TEST), Line.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return LINE;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return LINE_ARRAY;
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
    Class<? extends Line[]> getSingleDimensionArrayType() {
        return Line[].class;
    }

    @Override
    Class<? extends Line[][]> getTwoDimensionArrayType() {
        return Line[][].class;
    }

    @Override
    Line[] getExpectedSingleDimensionArray() {
        return new Line[]{Line.of(-10.42, 3.14, 5.24), Line.of(5.5, 3.2, 8)};
    }

    @Override
    Line[][] getExpectedTwoDimensionArray() {
        return new Line[][]{{Line.of(-10.42, 3.14, 5.24)}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"{ -10.42, 3.14, 5.24 }\",\"{5.5,3.2,8}\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"{-10.42,3.14,5.24}\"},{NULL}}";
    }

}
