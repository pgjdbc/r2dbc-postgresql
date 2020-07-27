package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.type.PostgresqlObjectId;

import java.util.List;

final class BoxCodec extends AbstractGeometryCodec<Box> {

    BoxCodec(ByteBufAllocator byteBufAllocator) {
        super(Box.class, PostgresqlObjectId.BOX, byteBufAllocator);
    }

    @Override
    Box doDecodeBinary(ByteBuf buffer) {
        double x1 = buffer.readDouble();
        double y1 = buffer.readDouble();
        double x2 = buffer.readDouble();
        double y2 = buffer.readDouble();
        return Box.of(Point.of(x1, y1), Point.of(x2, y2));
    }

    @Override
    Box doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        return Box.of(
            Point.of(Double.parseDouble(tokens.get(0)), Double.parseDouble(tokens.get(1))),
            Point.of(Double.parseDouble(tokens.get(2)), Double.parseDouble(tokens.get(3)))
        );
    }

    @Override
    ByteBuf doEncodeBinary(Box value) {
        return this.byteBufAllocator.buffer(32)
            .writeDouble(value.getA().getX())
            .writeDouble(value.getA().getY())
            .writeDouble(value.getB().getX())
            .writeDouble(value.getB().getY());
    }

}
