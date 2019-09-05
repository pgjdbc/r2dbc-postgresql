package io.r2dbc.postgresql.codec;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;

public class JsonCodecTest {
    
    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }
    
    @Test
    void decode() throws IOException {
        final String json = "{\"name\": \"John Doe\"}";
        final JsonCodec jsonCodec = new JsonCodec(TEST);
        final byte[] decodedBytes = jsonCodec.decode(ByteBufUtils.encode(TEST, json), JSON.getObjectId(), FORMAT_TEXT, byte[].class);

        assertThat(decodedBytes).isEqualTo(json.getBytes());
    }
    
    @Test
    void decodeNoByteBuf() {
        assertThat(new JsonCodec(TEST).decode(null, JSON.getObjectId(), FORMAT_TEXT, byte[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        final JsonCodec jsonCodec = new JsonCodec(TEST);
        
        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_TEXT)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_BINARY)).isFalse();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_BINARY)).isFalse();
    }
    
    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonCodec(TEST).doCanDecode(JSON, null))
            .withMessage("format must not be null");
    }
    
    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }
    
    @Test
    void doEncode() {
        final String json = "{\"name\":\"John Doe\"}";
        final JsonCodec jsonCodec = new JsonCodec(TEST);
        
        assertThat(jsonCodec.doEncode(json.getBytes()))
            .hasFormat(FORMAT_TEXT)
            .hasType(JSON.getObjectId())
            .hasValue(ByteBufUtils.encode(TEST, json));
    }
    
    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }
    
    @Test
    void encodeNull() {
        assertThat(new JsonCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, JSON.getObjectId(), NULL_VALUE));
    }

}
