package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.time.Month;
import java.util.Arrays;
import java.util.function.Consumer;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.*;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class MonthCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MonthCodec(null))
                .withMessage("byteBufAllocator must not be null");
    }


    @Test
    void decode() {
        forEveryMonth(m ->
                assertThat(new MonthCodec(TEST).decode(TEST.buffer().writeInt(m.getValue()), INT4, FORMAT_BINARY, Month.class)).isEqualTo(m));
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new MonthCodec(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, Month.class)).isNull();
    }

    @Test
    void doCanDecode() {
        MonthCodec codec = new MonthCodec(TEST);

        assertThat(codec.doCanDecode(INT4, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT2, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT8, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(NUMERIC, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MonthCodec(TEST).doCanDecode(null, FORMAT_BINARY))
                .withMessage("type must not be null");
    }

    @Test
    void doEncodeInt() {

        forEveryMonth(m -> {
            ParameterAssert.assertThat(new MonthCodec(TEST).doEncode(m))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT4.getObjectId())
                    .hasValue(TEST.buffer().writeInt(m.getValue()));
        });
    }

    @Test
    void doEncodeShort() {
        forEveryMonth(m -> {
            ParameterAssert.assertThat(new MonthCodec(TEST).doEncode(m, INT2))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT2.getObjectId())
                    .hasValue(TEST.buffer().writeShort(m.getValue()));
        });
    }

    @Test
    void doEncodeLong() {

        forEveryMonth(m -> {
            ParameterAssert.assertThat(new MonthCodec(TEST).doEncode(m, INT8))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT8.getObjectId())
                    .hasValue(TEST.buffer().writeLong(m.getValue()));
        });
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MonthCodec(TEST).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new MonthCodec(TEST).encode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new MonthCodec(TEST).encodeNull())
                .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    private void forEveryMonth(Consumer<Month> assertion) {
        Arrays.stream(Month.values()).forEach(assertion);
    }
}