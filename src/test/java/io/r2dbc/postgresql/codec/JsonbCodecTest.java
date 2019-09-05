package io.r2dbc.postgresql.codec;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;

public class JsonbCodecTest {
    
    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonbCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }
    
    @Test
    void decode() throws IOException {
        final String json = "{\"name\": \"John Doe\"}";
        final JsonbCodec jsonbCodec = new JsonbCodec(TEST);
        final byte[] decodedBytes = jsonbCodec.decode(ByteBufUtils.encode(TEST, json), JSONB.getObjectId(), FORMAT_BINARY, byte[].class);

        assertThat(decodedBytes).isEqualTo(json.getBytes());
    }
    
    @Test
    void decodeNoByteBuf() {
        assertThat(new JsonbCodec(TEST).decode(null, JSONB.getObjectId(), FORMAT_TEXT, byte[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        final JsonbCodec jsonbCodec = new JsonbCodec(TEST);
        
        assertThat(jsonbCodec.doCanDecode(JSONB, FORMAT_TEXT)).isTrue();
        assertThat(jsonbCodec.doCanDecode(JSONB, FORMAT_BINARY)).isFalse();
        assertThat(jsonbCodec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(jsonbCodec.doCanDecode(MONEY, FORMAT_BINARY)).isFalse();
    }
    
    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonbCodec(TEST).doCanDecode(JSONB, null))
            .withMessage("format must not be null");
    }
    
    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonbCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }
    
    @Test
    void doEncode() {
        final String json = "{\"name\":\"John Doe\"}";
        final JsonbCodec jsonbCodec = new JsonbCodec(TEST);
        final byte[] jsonBytes = json.getBytes();
        final ByteBuf encoded = TEST.buffer(jsonBytes.length + 1).writeByte(1).writeBytes(jsonBytes);
        
        assertThat(jsonbCodec.doEncode(jsonBytes))
            .hasFormat(FORMAT_BINARY)
            .hasType(JSONB.getObjectId())
            .hasValue(encoded);
    }
    
    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonbCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }
    
    @Test
    void encodeNull() {
        assertThat(new JsonbCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, JSONB.getObjectId(), NULL_VALUE));
    }

}
