package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.type.PostgresqlObjectId;

import java.util.List;

final class LsegCodec extends AbstractGeometryCodec<Lseg> {

    LsegCodec(ByteBufAllocator byteBufAllocator) {
        super(Lseg.class, PostgresqlObjectId.LSEG, byteBufAllocator);
    }

    @Override
    Lseg doDecodeBinary(ByteBuf byteBuffer) {
        return Lseg.of(
            Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()),
            Point.of(byteBuffer.readDouble(), byteBuffer.readDouble())
        );
    }

    @Override
    Lseg doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        return Lseg.of(
            Point.of(Double.parseDouble(tokens.get(0)), Double.parseDouble(tokens.get(1))),
            Point.of(Double.parseDouble(tokens.get(2)), Double.parseDouble(tokens.get(3)))
        );
    }

    @Override
    ByteBuf doEncodeBinary(Lseg value) {
        return this.byteBufAllocator
            .buffer(32)
            .writeDouble(value.getP1().getX())
            .writeDouble(value.getP1().getY())
            .writeDouble(value.getP2().getX())
            .writeDouble(value.getP2().getY());
    }

}
