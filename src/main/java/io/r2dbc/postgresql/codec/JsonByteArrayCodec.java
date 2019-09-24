package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;

final class JsonByteArrayCodec extends AbstractJsonCodec<byte[]> {

    private final ByteBufAllocator byteBufAllocator;

    JsonByteArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byte[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    byte[] doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends byte[]> type) {

        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

    @Override
    Parameter doEncode(byte[] value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(JSON, FORMAT_TEXT, () -> this.byteBufAllocator.buffer(value.length).writeBytes(value));
    }

}
