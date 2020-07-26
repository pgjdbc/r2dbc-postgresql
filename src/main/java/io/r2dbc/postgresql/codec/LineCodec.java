package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.type.PostgresqlObjectId;

import java.util.List;

final class LineCodec extends AbstractGeometryCodec<Line> {

    LineCodec(ByteBufAllocator byteBufAllocator) {
        super(Line.class, PostgresqlObjectId.LINE, byteBufAllocator);
    }

    @Override
    Line doDecodeBinary(ByteBuf byteBuffer) {
        double a = byteBuffer.readDouble();
        double b = byteBuffer.readDouble();
        double c = byteBuffer.readDouble();
        return Line.of(a, b, c);
    }

    @Override
    Line doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        double a = Double.parseDouble(tokens.get(0));
        double b = Double.parseDouble(tokens.get(1));
        double c = Double.parseDouble(tokens.get(2));
        return Line.of(a, b, c);
    }

    @Override
    ByteBuf doEncodeBinary(Line value) {
        return this.byteBufAllocator
            .buffer(24)
            .writeDouble(value.getA())
            .writeDouble(value.getB())
            .writeDouble(value.getC());
    }

}
