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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;

/**
 * @since 0.8.5
 */
final class CircleCodec extends AbstractGeometryCodec<Circle> implements ArrayCodecDelegate<Circle> {

    CircleCodec(ByteBufAllocator byteBufAllocator) {
        super(Circle.class, CIRCLE, byteBufAllocator);
    }

    @Override
    Circle doDecodeBinary(ByteBuf byteBuffer) {
        return Circle.of(Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()), byteBuffer.readDouble());
    }

    @Override
    Circle doDecodeText(String text) {
        TokenStream stream = getTokenStream(text);
        return Circle.of(Point.of(stream.nextDouble(), stream.nextDouble()), stream.nextDouble());
    }

    @Override
    ByteBuf doEncodeBinary(Circle value) {
        Point center = value.getCenter();
        return this.byteBufAllocator.buffer(lengthInBytes())
            .writeDouble(center.getX())
            .writeDouble(center.getY())
            .writeDouble(value.getRadius());
    }

    @Override
    public String encodeToText(Circle value) {
        return String.format("\"%s\"", value);
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return CIRCLE_ARRAY;
    }

    int lengthInBytes() {
        return 24;
    }

}
