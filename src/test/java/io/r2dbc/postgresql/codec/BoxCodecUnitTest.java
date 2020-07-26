package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BOX;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link BoxCodec}.
 */
public class BoxCodecUnitTest {

    private static final int dataType = BOX.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BoxCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Box box = Box.of(Point.of(1.9, 2.8), Point.of(3.7, 4.6));

        ByteBuf boxTextFormat = encode(TEST, "((1.9, 2.8),(3.7, 4.6))");
        assertThat(new BoxCodec(TEST).decode(boxTextFormat, dataType, FORMAT_TEXT, Box.class))
            .isEqualTo(box);

        ByteBuf boxByteFormat = TEST.buffer(32)
            .writeDouble(1.9).writeDouble(2.8)
            .writeDouble(3.7).writeDouble(4.6);
        assertThat(new BoxCodec(TEST).decode(boxByteFormat, dataType, FORMAT_BINARY, Box.class))
            .isEqualTo(box);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BoxCodec(TEST).decode(null, dataType, FORMAT_TEXT, Box.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BoxCodec codec = new BoxCodec(TEST);

        assertThat(codec.doCanDecode(BOX, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(POINT, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BoxCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BoxCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Box box = Box.of(Point.of(1.9, 2.8), Point.of(3.7, 4.6));

        ParameterAssert.assertThat(new BoxCodec(TEST).doEncode(box))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(32)
                .writeDouble(1.9)
                .writeDouble(2.8)
                .writeDouble(3.7)
                .writeDouble(4.6)
            );
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BoxCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new BoxCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
