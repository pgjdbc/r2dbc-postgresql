package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.type.PostgresqlObjectId;

import java.util.ArrayList;
import java.util.List;

final class PolygonCodec extends AbstractGeometryCodec<Polygon> {

    PolygonCodec(ByteBufAllocator byteBufAllocator) {
        super(Polygon.class, PostgresqlObjectId.POLYGON, byteBufAllocator);
    }

    @Override
    Polygon doDecodeBinary(ByteBuf byteBuffer) {
        int size = byteBuffer.readInt();
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()));
        }
        return Polygon.of(points);
    }

    @Override
    Polygon doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i += 2) {
            points.add(Point.of(Double.parseDouble(tokens.get(i)), Double.parseDouble(tokens.get(i + 1))));
        }
        return Polygon.of(points);
    }

    @Override
    ByteBuf doEncodeBinary(Polygon value) {
        List<Point> points = value.getPoints();
        ByteBuf buffer = this.byteBufAllocator
            .buffer(points.size() * 16 + 4)
            .writeInt(points.size());
        points.forEach(point -> buffer.writeDouble(point.getX()).writeDouble(point.getY()));
        return buffer;
    }

}
