package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.time.Period;
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

class PeriodCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PeriodCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decodeDays() {
        final int days = 4;

        final Period period = Period.ofDays(days);
        assertDecodeOfPeriod(period);
    }

    @Test
    void decodeJunkString() {
        final String junkString = "hello world";
        final ByteBuf buffer = TEST.buffer();

        final int charsWritten = buffer.writeCharSequence(junkString, Charset.defaultCharset());
        assertThat(charsWritten).isEqualTo(junkString.length());

        assertThatExceptionOfType(DateTimeParseException.class)
            .isThrownBy(() -> new PeriodCodec(TEST).decode(buffer, VARCHAR, FORMAT_TEXT, Period.class));
    }

    @Test
    void decodeMonths() {
        final int months = 3;

        final Period period = Period.ofMonths(months);
        assertDecodeOfPeriod(period);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new PeriodCodec(TEST).decode(null, VARCHAR.getObjectId(), FORMAT_TEXT, Period.class)).isNull();
    }

    @Test
    void decodeWeeks() {
        final int weeks = 7;

        final Period period = Period.ofWeeks(weeks);
        assertDecodeOfPeriod(period);
    }

    @Test
    void decodeYearsMonthsDays() {
        final int years = 5;
        final int months = 4;
        final int days = 7;

        final Period period = Period.of(years, months, days);
        assertDecodeOfPeriod(period);
    }

    @Test
    void doCanDecode() {
        final PeriodCodec codec = new PeriodCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(CHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(BPCHAR, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(NAME, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(TEXT, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PeriodCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PeriodCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PeriodCodec(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new PeriodCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, VARCHAR.getObjectId(), NULL_VALUE));
    }

    private static void assertDecodeOfPeriod(Period period) {
        final ByteBuf buffer = TEST.buffer();

        final int charsWritten = buffer.writeCharSequence(period.toString(), Charset.defaultCharset());
        assertThat(charsWritten).isEqualTo(period.toString().length());

        assertThat(new PeriodCodec(TEST)
            .decode(buffer, VARCHAR, FORMAT_TEXT, Period.class))
            .isEqualTo(period);
    }

}