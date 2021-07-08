package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Abstract class for unit testing {@link ArrayCodec}s. It provides
 * generic tests for array codecs.
 */
abstract class AbstractArrayCodecUnitTests<T> {

    ArrayCodec<T> codec;

    abstract ArrayCodec<T> createInstance();

    @BeforeEach
    void setUp() {
        codec = createInstance();
    }

    abstract PostgresqlObjectId getPostgresqlObjectId();

    abstract PostgresqlObjectId getArrayPostgresqlObjectId();

    abstract ByteBuf getSingleDimensionBinaryArray();

    abstract ByteBuf getTwoDimensionBinaryArray();

    abstract Class<? extends T[]> getSingleDimensionArrayType();

    abstract Class<? extends T[][]> getTwoDimensionArrayType();

    abstract T[] getExpectedSingleDimensionArray();

    abstract T[][] getExpectedTwoDimensionArray();

    abstract String getSingleDimensionStringInput();

    abstract String getTwoDimensionStringInput();

    @Test
    void decodeItem() {
        T[] expected = getExpectedSingleDimensionArray();
        assertThat(codec.decode(getSingleDimensionBinaryArray(), getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, getSingleDimensionArrayType()))
            .isEqualTo(expected);
        assertThat(codec.decode(encode(TEST, getSingleDimensionStringInput()), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getSingleDimensionArrayType()))
            .isEqualTo(expected);
    }

    @Test
    void decodeItem_textArray() {
        assertThat(codec.decode(encode(TEST, getSingleDimensionStringInput()), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getSingleDimensionArrayType()))
            .isEqualTo(getExpectedSingleDimensionArray());
    }

    @Test
    @SuppressWarnings("unchecked")
    void decodeItem_emptyArray() {
        T[][] empty = (T[][]) new Object[][]{};
        assertThat(codec.decode(encode(TEST, "{}"), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getTwoDimensionArrayType()))
            .isEqualTo(empty);
    }

    @Test
    @SuppressWarnings("unchecked")
    void decodeItem_emptyBinaryArray() {
        ByteBuf buf = TEST
            .buffer()
            .writeInt(0)
            .writeInt(0)
            .writeInt(getPostgresqlObjectId().getObjectId());

        T[][] empty = (T[][]) new Object[][]{};
        assertThat(codec.decode(buf, getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, getTwoDimensionArrayType()))
            .isEqualTo(empty);
    }

    @Test
    void decodeItem_expectedLessDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, "{" + getSingleDimensionStringInput() + "}"), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getSingleDimensionArrayType()))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedLessDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(getTwoDimensionBinaryArray(), getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, getSingleDimensionArrayType()))
            .withMessage("Dimensions mismatch: 1 expected, but 2 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(encode(TEST, getSingleDimensionStringInput()), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getTwoDimensionArrayType()))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_expectedMoreDimensionsInBinaryArray() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> codec.decode(getSingleDimensionBinaryArray(), getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, getTwoDimensionArrayType()))
            .withMessage("Dimensions mismatch: 2 expected, but 1 returned from DB");
    }

    @Test
    void decodeItem_twoDimensionalArrayWithNull() {
        assertThat(codec.decode(encode(TEST, getTwoDimensionStringInput()), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, getTwoDimensionArrayType()))
            .isEqualTo(getExpectedTwoDimensionArray());
    }

    @Test
    void decodeItem_twoDimensionalBinaryArrayWithNull() {
        assertThat(codec.decode(getTwoDimensionBinaryArray(), getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, getTwoDimensionArrayType()))
            .isEqualTo(getExpectedTwoDimensionArray());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        Codec genericCodec = codec;
        assertThat(genericCodec.canDecode(getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, Object.class)).isTrue();
        T[] expected = getExpectedSingleDimensionArray();

        assertThat(genericCodec.decode(getSingleDimensionBinaryArray(), getArrayPostgresqlObjectId().getObjectId(), FORMAT_BINARY, Object.class))
            .isEqualTo(expected);
        assertThat(genericCodec.decode(encode(TEST, getSingleDimensionStringInput()), getArrayPostgresqlObjectId().getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(expected);
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(getPostgresqlObjectId(), FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(getArrayPostgresqlObjectId(), FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(getArrayPostgresqlObjectId(), FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        ParameterAssert.assertThat(codec.encodeArray(() -> encode(TEST, getSingleDimensionStringInput()), getArrayPostgresqlObjectId()))
            .hasFormat(FORMAT_TEXT)
            .hasType(getArrayPostgresqlObjectId().getObjectId())
            .hasValue(encode(TEST, getSingleDimensionStringInput()));
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, getArrayPostgresqlObjectId().getObjectId(), NULL_VALUE));
    }

}
