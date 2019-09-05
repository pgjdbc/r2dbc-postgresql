package io.r2dbc.postgresql.codec;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;

public class JsonbCodec extends AbstractCodec<byte[]> {
    
    private final ByteBufAllocator byteBufAllocator;

    public JsonbCodec(ByteBufAllocator byteBufAllocator) {
        super(byte[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }
    
    @Override
    public Parameter encodeNull() {
        return createNull(JSONB, FORMAT_BINARY);
    }
    
    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");
        
        return FORMAT_TEXT == format && JSONB == type;
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
        final ByteBuf encoded = this.byteBufAllocator.buffer(value.length + 1).writeByte(1).writeBytes(value);
        
        return create(JSONB, FORMAT_BINARY, Flux.just(encoded));
        
    }

}
