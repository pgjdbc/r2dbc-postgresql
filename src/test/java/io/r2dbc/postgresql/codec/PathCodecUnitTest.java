package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.LINE;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.PATH;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PathCodec}.
 */
public class PathCodecUnitTest {

    private static final int dataType = PATH.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PathCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Path closedPath = Path.of(false, Point.of(-10.42, 3.14), Point.of(10.42, -3.14));
        Path openPath = Path.of(true, Point.of(-10.42, 3.14), Point.of(10.42, -3.14));

        assertThat(new PathCodec(TEST).decode(encode(TEST, "((-10.42,3.14),(10.42,-3.14))"), dataType, FORMAT_TEXT, Path.class)).isEqualTo(closedPath);
        assertThat(new PathCodec(TEST).decode(encode(TEST, "[(-10.42,3.14),(10.42,-3.14)]"), dataType, FORMAT_TEXT, Path.class)).isEqualTo(openPath);

        ByteBuf closedPathByteFormat = TEST.buffer(37)
            .writeBoolean(false)
            .writeInt(2)
            .writeDouble(-10.42)
            .writeDouble(3.14)
            .writeDouble(10.42)
            .writeDouble(-3.14);
        assertThat(new PathCodec(TEST).decode(closedPathByteFormat, dataType, FORMAT_BINARY, Path.class)).isEqualTo(closedPath);
        ByteBuf openPathByteFormat = TEST.buffer(37)
            .writeBoolean(true)
            .writeInt(2)
            .writeDouble(-10.42)
            .writeDouble(3.14)
            .writeDouble(10.42)
            .writeDouble(-3.14);
        assertThat(new PathCodec(TEST).decode(openPathByteFormat, dataType, FORMAT_BINARY, Path.class)).isEqualTo(openPath);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new PathCodec(TEST).decode(null, dataType, FORMAT_TEXT, Path.class)).isNull();
    }

    @Test
    void doCanDecode() {
        PathCodec codec = new PathCodec(TEST);
        assertThat(codec.doCanDecode(PATH, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(PATH, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(LINE, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(POINT, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PathCodec(TEST).doCanDecode(PATH, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PathCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Path closedPath = Path.of(false, Point.of(-10.42, 3.14), Point.of(10.42, -3.14));
        ParameterAssert.assertThat(new PathCodec(TEST).doEncode(closedPath))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(37)
                .writeBoolean(false)
                .writeInt(2)
                .writeDouble(-10.42)
                .writeDouble(3.14)
                .writeDouble(10.42)
                .writeDouble(-3.14)
            );

        Path openPath = Path.of(true, Point.of(-10.42, 3.14), Point.of(10.42, -3.14));
        ParameterAssert.assertThat(new PathCodec(TEST).doEncode(openPath))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(37)
                .writeBoolean(true)
                .writeInt(2)
                .writeDouble(-10.42)
                .writeDouble(3.14)
                .writeDouble(10.42)
                .writeDouble(-3.14)
            );
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PathCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new PathCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }
    
}
