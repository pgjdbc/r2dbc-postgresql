package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.function.Consumer;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.*;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class DayOfWeekCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(null))
                .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        forEveryDayOfWeek(d ->
                assertThat(new DayOfWeekCodec(TEST).decode(TEST.buffer().writeInt(d.getValue()), INT4, FORMAT_BINARY, DayOfWeek.class)).isEqualTo(d));
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new DayOfWeekCodec(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, DayOfWeek.class)).isNull();
    }

    @Test
    void doCanDecode() {
        DayOfWeekCodec codec = new DayOfWeekCodec(TEST);

        assertThat(codec.doCanDecode(INT4, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT2, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT8, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(NUMERIC, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).doCanDecode(null, FORMAT_BINARY))
                .withMessage("type must not be null");
    }

    @Test
    void doEncodeInt() {

        forEveryDayOfWeek(d -> {
            ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT4.getObjectId())
                    .hasValue(TEST.buffer().writeInt(d.getValue()));
        });
    }

    @Test
    void doEncodeShort() {
        forEveryDayOfWeek(d -> {
            ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d, INT2))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT2.getObjectId())
                    .hasValue(TEST.buffer().writeShort(d.getValue()));
        });
    }

    @Test
    void doEncodeLong() {

        forEveryDayOfWeek(d -> {
            ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d, INT8))
                    .hasFormat(FORMAT_BINARY)
                    .hasType(INT8.getObjectId())
                    .hasValue(TEST.buffer().writeLong(d.getValue()));
        });
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).encode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new DayOfWeekCodec(TEST).encodeNull())
                .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    private void forEveryDayOfWeek(Consumer<DayOfWeek> assertion) {
        Arrays.stream(DayOfWeek.values()).forEach(assertion);
    }
}