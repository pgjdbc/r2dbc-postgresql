package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link JsonByteArrayCodec}.
 */
final class JsonByteArrayCodecUnitTests {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonByteArrayCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        String json = "{\"name\": \"John Doe\"}";
        JsonByteArrayCodec jsonCodec = new JsonByteArrayCodec(TEST);
        byte[] decodedBytes = jsonCodec.decode(ByteBufUtils.encode(TEST, json), JSON.getObjectId(), FORMAT_TEXT, byte[].class);

        assertThat(decodedBytes).isEqualTo(json.getBytes());
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new JsonByteArrayCodec(TEST).decode(null, JSON.getObjectId(), FORMAT_TEXT, byte[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        JsonByteArrayCodec jsonCodec = new JsonByteArrayCodec(TEST);

        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_TEXT)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_BINARY)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSONB, FORMAT_TEXT)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSONB, FORMAT_BINARY)).isTrue();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_BINARY)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonByteArrayCodec(TEST).doCanDecode(JSON, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonByteArrayCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        String json = "{\"name\":\"John Doe\"}";
        JsonByteArrayCodec jsonCodec = new JsonByteArrayCodec(TEST);

        assertThat(jsonCodec.doEncode(json.getBytes()))
            .hasFormat(FORMAT_TEXT)
            .hasType(JSON.getObjectId())
            .hasValue(ByteBufUtils.encode(TEST, json));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonByteArrayCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new JsonByteArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, JSONB.getObjectId(), NULL_VALUE));
    }

}
