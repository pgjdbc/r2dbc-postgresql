package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NAME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class YearMonthCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearMonthCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        final YearMonth yearMonth = YearMonth.now();

        final ByteBuf buffer = TEST.buffer();
        
        final int charsWritten = buffer.writeCharSequence(yearMonth.toString(), Charset.defaultCharset());
        assertThat(charsWritten).isEqualTo(yearMonth.toString().length());

        assertThat(new YearMonthCodec(TEST)
            .decode(buffer, VARCHAR, FORMAT_TEXT, YearMonth.class))
            .isEqualTo(yearMonth);
    }

    @Test
    void decodeJunkString() {
        final String junkString = "hello world";
        final ByteBuf buffer = TEST.buffer();

        final int charsWritten = buffer.writeCharSequence(junkString, Charset.defaultCharset());
        assertThat(charsWritten).isEqualTo(junkString.length());

        assertThatExceptionOfType(DateTimeParseException.class)
            .isThrownBy(() -> new YearMonthCodec(TEST).decode(buffer, VARCHAR, FORMAT_TEXT, YearMonth.class));
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new YearMonthCodec(TEST).decode(null, VARCHAR.getObjectId(), FORMAT_TEXT, YearMonth.class)).isNull();
    }

    @Test
    void doCanDecode() {
        YearMonthCodec codec = new YearMonthCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(CHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(BPCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(NAME, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TEXT, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearMonthCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearMonthCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new YearMonthCodec(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new YearMonthCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, VARCHAR.getObjectId(), NULL_VALUE));
    }

}