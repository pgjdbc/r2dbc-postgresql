/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON_ARRAY;

/**
 * @since 0.8.5
 */
final class PolygonCodec extends AbstractGeometryCodec<Polygon> implements ArrayCodecDelegate<Polygon> {

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
        TokenStream stream = getTokenStream(text);
        List<Point> points = new ArrayList<>();

        while (stream.hasNext()) {
            points.add(Point.of(stream.nextDouble(), stream.nextDouble()));
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

    @Override
    public String encodeToText(Polygon value) {
        return String.format("\"%s\"", value);
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return POLYGON_ARRAY;
    }

}
