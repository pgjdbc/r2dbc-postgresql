package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.time.Year;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class YearCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Year year = Year.now();

        assertThat(new YearCodec(TEST).decode(TEST.buffer().writeInt(year.getValue()), INT4, FORMAT_BINARY, Year.class))
            .isEqualTo(year);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new YearCodec(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, Year.class)).isNull();
    }

    @Test
    void doCanDecode() {
        YearCodec codec = new YearCodec(TEST);

        assertThat(codec.doCanDecode(INT4, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT2, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT8, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(NUMERIC, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearCodec(TEST).doCanDecode(null, FORMAT_BINARY))
            .withMessage("type must not be null");
    }

    @Test
    void doEncodeInt() {
        Year year = Year.now();

        ParameterAssert.assertThat(new YearCodec(TEST).doEncode(year))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT4.getObjectId())
            .hasValue(TEST.buffer().writeInt(year.getValue()));
    }

    @Test
    void doEncodeLong() {
        Year year = Year.now();

        ParameterAssert.assertThat(new YearCodec(TEST).doEncode(year, INT8))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT8.getObjectId())
            .hasValue(TEST.buffer().writeLong(year.getValue()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void doEncodeShort() {
        Year year = Year.now();

        ParameterAssert.assertThat(new YearCodec(TEST).doEncode(year, INT2))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT2.getObjectId())
            .hasValue(TEST.buffer().writeShort(year.getValue()));
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearCodec(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new YearCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    @Test
    void myTest() {

    }

}