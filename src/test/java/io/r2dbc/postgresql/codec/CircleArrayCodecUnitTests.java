package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ArrayCodec<Circle>}.
 */
final class CircleArrayCodecUnitTests extends AbstractArrayCodecUnitTests<Circle> {

    private static final int dataType = CIRCLE_ARRAY.getObjectId();

    private final ByteBuf SINGLE_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1) // num of dimensions
        .writeInt(0) // flag: has nulls
        .writeInt(718) // oid
        .writeInt(2) // num of elements
        .writeInt(1) // ignore Lower Bound
        .writeInt(24) // length of element
        .writeDouble(1.2) // center point x
        .writeDouble(123.1) // center point y
        .writeDouble(10.0) // radius
        .writeInt(24) // length of element
        .writeDouble(-2.4) // center point x
        .writeDouble(-456.2) // center point y
        .writeDouble(20.0); // radius

    private final ByteBuf TWO_DIM_BINARY_ARRAY = TEST
        .buffer()
        .writeInt(2) // num of dims
        .writeInt(1) // flag: has nulls
        .writeInt(718) // oid
        .writeInt(2) // dim 1 length
        .writeInt(1) // dim 1 lower bound
        .writeInt(1) // dim 2 length
        .writeInt(1) // dim 2 lower bound
        .writeInt(24) // length of element
        .writeDouble(1.2) // center point x
        .writeDouble(123.1) // center point y
        .writeDouble(10.0) // radius
        .writeInt(-1); // length of null element

    @Override
    ArrayCodec<Circle> createInstance() {
        return new ArrayCodec<>(TEST, CIRCLE_ARRAY, new CircleCodec(TEST), Circle.class);
    }

    @Override
    PostgresqlObjectId getPostgresqlObjectId() {
        return CIRCLE;
    }

    @Override
    PostgresqlObjectId getArrayPostgresqlObjectId() {
        return CIRCLE_ARRAY;
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
    Class<? extends Circle[]> getSingleDimensionArrayType() {
        return Circle[].class;
    }

    @Override
    Class<? extends Circle[][]> getTwoDimensionArrayType() {
        return Circle[][].class;
    }

    @Override
    Circle[] getExpectedSingleDimensionArray() {
        return new Circle[]{Circle.of(Point.of(1.2, 123.1), 10), Circle.of(Point.of(-2.4, -456.2), 20)};
    }

    @Override
    Circle[][] getExpectedTwoDimensionArray() {
        return new Circle[][]{{Circle.of(Point.of(1.2, 123.1), 10)}, {null}};
    }

    @Override
    String getSingleDimensionStringInput() {
        return "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}";
    }

    @Override
    String getTwoDimensionStringInput() {
        return "{{\"((1.2, 123.1), 10)\"},{NULL}}";
    }

    @Test
    @Override
    void decodeItem_textArray() {
        Circle[] expected = getExpectedSingleDimensionArray();
        assertThat(codec.decode(encode(TEST, "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"((1.2, 123.1), 10)\",\"((-2.4, -456.2), 20)\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"(1.2, 123.1), 10\",\"(-2.4, -456.2), 20\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"1.2, 123.1, 10\",\"-2.4, -456.2, 20\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
    }

}