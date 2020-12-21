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

/**
 * @since 0.8.5
 */
final class PathCodec extends AbstractGeometryCodec<Path> {

    PathCodec(ByteBufAllocator byteBufAllocator) {
        super(Path.class, PostgresqlObjectId.PATH, byteBufAllocator);
    }

    @Override
    Path doDecodeBinary(ByteBuf byteBuffer) {
        boolean closed = byteBuffer.readBoolean();
        int size = byteBuffer.readInt();
        List<Point> points = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            points.add(Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()));
        }

        return Path.of(!closed, points);
    }

    @Override
    Path doDecodeText(String text) {
        boolean open = text.startsWith("[");
        TokenStream stream = getTokenStream(text);
        List<Point> points = new ArrayList<>();

        while (stream.hasNext()) {
            points.add(Point.of(stream.nextDouble(), stream.nextDouble()));
        }

        return Path.of(open, points);
    }

    @Override
    ByteBuf doEncodeBinary(Path value) {
        List<Point> points = value.getPoints();

        ByteBuf buffer = this.byteBufAllocator
            .buffer(points.size() * 16 + 5)
            .writeBoolean(!value.isOpen())
            .writeInt(points.size());

        points.forEach(point -> buffer.writeDouble(point.getX()).writeDouble(point.getY()));

        return buffer;
    }

}
