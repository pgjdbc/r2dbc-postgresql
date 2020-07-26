package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.List;

abstract class AbstractGeometryCodec<T> extends AbstractCodec<T> {

    protected final PostgresqlObjectId postgresqlObjectId;

    protected final ByteBufAllocator byteBufAllocator;

    AbstractGeometryCodec(Class<T> type, PostgresqlObjectId postgresqlObjectId, ByteBufAllocator byteBufAllocator) {
        super(type);
        this.postgresqlObjectId = Assert.requireNonNull(postgresqlObjectId, "postgresqlObjectId must not be null");
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    abstract T doDecodeBinary(ByteBuf byteBuffer);

    abstract T doDecodeText(String text);

    abstract ByteBuf doEncodeBinary(T value);

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");
        return postgresqlObjectId == type;
    }

    @Override
    T doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends T> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");
        if (format == Format.FORMAT_BINARY) {
            return doDecodeBinary(buffer);
        }
        return doDecodeText(ByteBufUtils.decode(buffer));
    }

    @Override
    Parameter doEncode(T value) {
        Assert.requireNonNull(value, "value must not be null");
        return create(this.postgresqlObjectId, Format.FORMAT_BINARY, () -> doEncodeBinary(value));
    }

    @Override
    public Parameter encodeNull() {
        return createNull(postgresqlObjectId, Format.FORMAT_BINARY);
    }

    protected List<String> tokenizeTextData(String string) {
        List<String> tokens = new ArrayList<>();

        for (int p = 0, s = 0; p < string.length(); p++) {
            char c = string.charAt(p);

            if (c == '(' || c == '[' || c == '<' || c == '{') {
                s++;
                continue;
            }

            if (c == ',' || c == ')' || c == ']' || c == '>' || c == '}') {
                if (s != p) {
                    tokens.add(string.substring(s, p));
                    s = p + 1;
                } else {
                    s++;
                }
            }
        }

        return tokens;
    }

}
