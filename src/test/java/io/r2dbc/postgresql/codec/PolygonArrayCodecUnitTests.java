package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Unit tests for {@link ArrayCodec<Polygon>}.
 */
final class PolygonArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Polygon> {

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(604) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(4 + 16 * /* number of points */2) // number of bytes to follow
        .writeInt(2) // number of points
        .writeDouble(-10.42) // point 1 x
        .writeDouble(3.14) // point 1 y
        .writeDouble(10.42) // point 2 x
        .writeDouble(-3.14) // point 2 y
        .writeInt(4 + 16 * /* number of points */4) // number of bytes to follow
        .writeInt(4) // number of points
        .writeDouble(10.42) // point 1 x
        .writeDouble(3.14) // point 1 y
        .writeDouble(10.42) // point 2 x
        .writeDouble(-3.14) // point 2 y
        .writeDouble(-10.42) // point 3 x
        .writeDouble(-3.14) // point 3 y
        .writeDouble(-10.42) // point 4 x
        .writeDouble(3.14); // point 4 y

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(604) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(4 + 16 * /* number of points */2) // number of bytes to follow
        .writeInt(2) // number of points
        .writeDouble(-10.42) // point 1 x
        .writeDouble(3.14) // point 1 y
        .writeDouble(10.42) // point 2 x
        .writeDouble(-3.14) // point 2 y
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Polygon> createInstance() {
        return new ArrayCodec<>(TEST, POLYGON_ARRAY, new PolygonCodec(TEST), Polygon.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return POLYGON;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return POLYGON_ARRAY;
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
    Class<? extends Polygon[]> getSingleDimensionArrayType() {
        return Polygon[].class;
    }

    @Override
    Class<? extends Polygon[][]> getTwoDimensionArrayType() {
        return Polygon[][].class;
    }

    @Override
    Polygon[] getExpectedSingleDimensionArray() {
        return new Polygon[]{Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)), Polygon.of(Point.of(10.42, 3.14), Point.of(10.42, -3.14), Point.of(-10.42, -3.14), Point.of(-10.42, 3.14))};
    }

    @Override
    Polygon[][] getExpectedTwoDimensionArray() {
        return new Polygon[][]{{Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14))}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"((-10.42,3.14),(10.42,-3.14))\",\"((10.42,3.14),(10.42,-3.14),(-10.42,-3.14),(-10.42,3.14))\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"((-10.42,3.14),(10.42,-3.14))\"},{NULL}}";
    }

}
