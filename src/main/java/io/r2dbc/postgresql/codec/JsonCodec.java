package io.r2dbc.postgresql.codec;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.core.publisher.Flux;

public class JsonCodec extends AbstractCodec<byte[]> {
    
    private final ByteBufAllocator byteBufAllocator;

    public JsonCodec(ByteBufAllocator byteBufAllocator) {
        super(byte[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(JSON, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        
        return FORMAT_TEXT == format && JSON == type;
    }

    @Override
    byte[] doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends byte[]> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        
        final byte[] bytes = new byte[buffer.readableBytes()];
        final int readIndex = buffer.readerIndex();
        buffer.getBytes(readIndex, bytes);
        
        return bytes;
    }

    @Override
    Parameter doEncode(byte[] value) {
        Assert.requireNonNull(value, "value must not be null");
        final String json = new String(value);
        final ByteBuf encoded = ByteBufUtils.encode(this.byteBufAllocator, json);
        
        return create(JSON, FORMAT_TEXT, Flux.just(encoded));
    }

}
