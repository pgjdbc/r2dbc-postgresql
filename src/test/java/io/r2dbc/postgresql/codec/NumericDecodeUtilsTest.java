package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.util.TestByteBufAllocator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.util.ByteConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NumericDecodeUtils}.
 */
class NumericDecodeUtilsTest {

    static Stream<Object[]> bigDecimalValues() {
        return Stream.of(
            new Object[]{new BigDecimal("0.1")},
            new Object[]{new BigDecimal("0.10")},
            new Object[]{new BigDecimal("0.01")},
            new Object[]{new BigDecimal("0.001")},
            new Object[]{new BigDecimal("0.0001")},
            new Object[]{new BigDecimal("0.00001")},
            new Object[]{new BigDecimal("-0.1")},
            new Object[]{new BigDecimal("-0.10")},
            new Object[]{new BigDecimal("-0.01")},
            new Object[]{new BigDecimal("-0.002")},
            new Object[]{new BigDecimal("-0.0033")},
            new Object[]{new BigDecimal("-0.004343")},
            new Object[]{new BigDecimal("1.0")},
            new Object[]{new BigDecimal("0.000000000000000000000000000000000000000000000000000")},
            new Object[]{new BigDecimal("0.100000000000000000000000000000000000000000000009900")},
            new Object[]{new BigDecimal("-1.0")},
            new Object[]{new BigDecimal("-1")},
            new Object[]{new BigDecimal("1.2")},
            new Object[]{new BigDecimal("-2.05")},
            new Object[]{new BigDecimal("0.000000000000000000000000000990")},
            new Object[]{new BigDecimal("-0.000000000000000000000000000990")},
            new Object[]{new BigDecimal("10.0000000000099")},
            new Object[]{new BigDecimal(".10000000000000")},
            new Object[]{new BigDecimal("1.10000000000000")},
            new Object[]{new BigDecimal("99999.2")},
            new Object[]{new BigDecimal("99999")},
            new Object[]{new BigDecimal("-99999.2")},
            new Object[]{new BigDecimal("-99999")},
            new Object[]{new BigDecimal("2147483647")},
            new Object[]{new BigDecimal("-2147483648")},
            new Object[]{new BigDecimal("2147483648")},
            new Object[]{new BigDecimal("-2147483649")},
            new Object[]{new BigDecimal("9223372036854775807")},
            new Object[]{new BigDecimal("-9223372036854775808")},
            new Object[]{new BigDecimal("9223372036854775808")},
            new Object[]{new BigDecimal("-9223372036854775809")},
            new Object[]{new BigDecimal("10223372036850000000")},
            new Object[]{new BigDecimal("19223372036854775807")},
            new Object[]{new BigDecimal("19223372036854775807.300")},
            new Object[]{new BigDecimal("-19223372036854775807.300")},
            new Object[]{new BigDecimal(BigInteger.valueOf(1234567890987654321L), -1)},
            new Object[]{new BigDecimal(BigInteger.valueOf(1234567890987654321L), -5)},
            new Object[]{new BigDecimal(BigInteger.valueOf(-1234567890987654321L), -3)},
            new Object[]{new BigDecimal(BigInteger.valueOf(6), -8)},
            new Object[]{new BigDecimal("30000")},
            new Object[]{new BigDecimal("40000").setScale(15, RoundingMode.UNNECESSARY)},
            new Object[]{new BigDecimal("20000.000000000000000000")},
            new Object[]{new BigDecimal("9990000").setScale(8, RoundingMode.UNNECESSARY)},
            new Object[]{new BigDecimal("1000000").setScale(31, RoundingMode.UNNECESSARY)},
            new Object[]{new BigDecimal("10000000000000000000000000000000000000").setScale(14, RoundingMode.UNNECESSARY)},
            new Object[]{new BigDecimal("90000000000000000000000000000000000000")},
            new Object[]{new BigDecimal("1234567890.12")},
            new Object[]{new BigDecimal("-3.141592653590")},
            new Object[]{new BigDecimal("3.141592653590")},
            new Object[]{new BigDecimal("-0.141592653590")},
            new Object[]{new BigDecimal("0.141592653590")}
        );
    }

    @MethodSource("bigDecimalValues")
    @ParameterizedTest
    void decodeBinary2(BigDecimal value) {
        ByteBuf byteBuf = TestByteBufAllocator.TEST.buffer();
        byteBuf.writeBytes(ByteConverter.numeric(value));
        assertThat(NumericDecodeUtils.decodeBinary(byteBuf)).isEqualByComparingTo(value);
        byteBuf.release();
    }

}
