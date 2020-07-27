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

import java.util.List;

import static io.r2dbc.postgresql.type.PostgresqlObjectId.CIRCLE;

final class CircleCodec extends AbstractGeometryCodec<Circle> {

    CircleCodec(ByteBufAllocator byteBufAllocator) {
        super(Circle.class, CIRCLE, byteBufAllocator);
    }

    @Override
    Circle doDecodeBinary(ByteBuf byteBuffer) {
        double x = byteBuffer.readDouble();
        double y = byteBuffer.readDouble();
        double r = byteBuffer.readDouble();
        return new Circle(Point.of(x, y), r);
    }

    @Override
    Circle doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        double x = Double.parseDouble(tokens.get(0));
        double y = Double.parseDouble(tokens.get(1));
        double r = Double.parseDouble(tokens.get(2));
        return new Circle(Point.of(x, y), r);
    }

    @Override
    ByteBuf doEncodeBinary(Circle value) {
        Point center = value.getCenter();
        return this.byteBufAllocator.buffer(lengthInBytes())
            .writeDouble(center.getX())
            .writeDouble(center.getY())
            .writeDouble(value.getRadius());
    }

    int lengthInBytes() {
        return 24;
    }

}
