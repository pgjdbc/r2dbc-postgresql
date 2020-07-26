package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.type.PostgresqlObjectId;

import java.util.ArrayList;
import java.util.List;

final class PathCodec extends AbstractGeometryCodec<Path> {

    PathCodec(ByteBufAllocator byteBufAllocator) {
        super(Path.class, PostgresqlObjectId.PATH, byteBufAllocator);
    }

    @Override
    Path doDecodeBinary(ByteBuf byteBuffer) {
        boolean isOpen = byteBuffer.readBoolean();
        int size = byteBuffer.readInt();
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()));
        }
        return Path.of(isOpen, points);
    }

    @Override
    Path doDecodeText(String text) {
        boolean isOpen = text.startsWith("[");
        List<String> tokens = tokenizeTextData(text);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i += 2) {
            points.add(Point.of(Double.parseDouble(tokens.get(i)), Double.parseDouble(tokens.get(i + 1))));
        }
        return Path.of(isOpen, points);
    }

    @Override
    ByteBuf doEncodeBinary(Path value) {
        List<Point> points = value.getPoints();
        ByteBuf buffer = this.byteBufAllocator
            .buffer(points.size() * 16 + 5)
            .writeBoolean(value.isOpen())
            .writeInt(points.size());
        points.forEach(point -> buffer.writeDouble(point.getX()).writeDouble(point.getY()));
        return buffer;
    }

}
