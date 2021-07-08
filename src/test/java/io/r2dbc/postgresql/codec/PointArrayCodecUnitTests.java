package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POINT_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Point>}.
 */
final class PointArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Point> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(600) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(16) // length of element
        .writeDouble(1.2) // point x
        .writeDouble(123.1) // point y
        .writeInt(16) // length of element
        .writeDouble(-2.4) // point x
        .writeDouble(-456.2); // point y

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(600) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(16) // length of element
        .writeDouble(1.2) // point x
        .writeDouble(123.1) // point y
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Point> createInstance() {
        return new ArrayCodec<>(TEST, POINT_ARRAY, new PointCodec(TEST), Point.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return POINT;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return POINT_ARRAY;
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
    Class<? extends Point[]> getSingleDimensionArrayType() {
        return Point[].class;
    }

    @Override
    Class<? extends Point[][]> getTwoDimensionArrayType() {
        return Point[][].class;
    }

    @Override
    Point[] getExpectedSingleDimensionArray() {
        return new Point[]{Point.of(1.2, 123.1), Point.of(-2.4, -456.2)};
    }

    @Override
    Point[][] getExpectedTwoDimensionArray() {
        return new Point[][]{{Point.of(1.2, 123.1)}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"(1.2,123.1)\",\"(-2.4,-456.2)\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"(1.2,123.1)\"},{NULL}}";
    }

}
