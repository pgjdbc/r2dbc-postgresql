package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ArrayCodec<Circle>}.
 */
final class CircleArrayCodecUnitTests {

    private static final int dataType = CIRCLE_ARRAY.getObjectId();

    private ArrayCodec<Circle> codec;

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

    @BeforeEach
    void setup() {
        codec = new ArrayCodec<>(TEST, CIRCLE_ARRAY, new CircleCodec(TEST), Circle.class);
    }

    @Test
    void decodeItem() {
        assertThat(codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Circle[].class))
            .isEqualTo(new Circle[]{Circle.of(Point.of(1.2, 123.1), 10), Circle.of(Point.of(-2.4, -456.2), 20)});
    }

    @Test
    void decodeItem_textArray() {
        Circle[] expected = {Circle.of(Point.of(1.2, 123.1), 10), Circle.of(Point.of(-2.4, -456.2), 20)};
        assertThat(codec.decode(encode(TEST, "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"((1.2, 123.1), 10)\",\"((-2.4, -456.2), 20)\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"(1.2, 123.1), 10\",\"(-2.4, -456.2), 20\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, "{\"1.2, 123.1, 10\",\"-2.4, -456.2, 20\"}"), dataType, FORMAT_TEXT, Circle[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeItem_emptyArray() {
        assertThat(codec.decode(encode(TEST, "{}"), dataType, FORMAT_TEXT, Circle[][].class))
            .isEqualTo(new Circle[][]{});
    }

    @Test
    void decodeItem_emptyBinaryArray() {
        ByteBuf buf = TEST
            .buffer()
            .writeInt(0)
            .writeInt(0)
            .writeInt(718);

        assertThat(codec.decode(buf, dataType, FORMAT_BINARY, Circle[][].class))
            .isEqualTo(new Circle[][]{});
    }

    @Test
    void decodeItem_expectedLessDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{{\"((1.2, 123.1), 10)\"}}"), dataType, FORMAT_TEXT, Circle[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Circle[].class))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{\"1.2, 123.1, 10\",\"-2.4, -456.2, 20\"}"), dataType, FORMAT_TEXT, Circle[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Circle[][].class))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_twoDimensionalArrayWithNull() {
        assertThat(codec.decode(encode(TEST, "{{\"((1.2, 123.1), 10)\"},{NULL}}"), dataType, FORMAT_TEXT, Circle[][].class))
            .isEqualTo(new Circle[][]{{Circle.of(Point.of(1.2, 123.1), 10)}, {null}});
    }

    @Test
    void decodeItem_twoDimensionalBinaryArrayWithNull() {
        assertThat(codec.decode(TWO_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Circle[][].class))
            .isEqualTo(new Circle[][]{{Circle.of(Point.of(1.2, 123.1), 10)}, {null}});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec genericCodec = codec;
        assertThat(genericCodec.canDecode(CIRCLE_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isTrue();
        Circle[] expected = {Circle.of(Point.of(1.2, 123.1), 10), Circle.of(Point.of(-2.4, -456.2), 20)};

        assertThat(genericCodec.decode(SINGLE_DIM_BINARY_ARRAY, dataType, FORMAT_BINARY, Object.class))
            .isEqualTo(expected);
        assertThat(genericCodec.decode(encode(TEST, "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}"), dataType, FORMAT_TEXT, Object.class))
            .isEqualTo(expected);
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(CIRCLE, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(CIRCLE_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(CIRCLE_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        ParameterAssert.assertThat(codec.encodeArray(() -> encode(TEST, "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}"), CIRCLE_ARRAY))
            .hasFormat(FORMAT_TEXT)
            .hasType(CIRCLE_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{\"<(1.2, 123.1), 10>\",\"<(-2.4, -456.2), 20>\"}"));
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, CIRCLE_ARRAY.getObjectId(), NULL_VALUE));
    }

}