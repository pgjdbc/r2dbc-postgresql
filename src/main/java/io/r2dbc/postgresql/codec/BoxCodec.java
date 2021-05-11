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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX_ARRAY;

/**
 * @since 0.8.5
 */
final class BoxCodec extends AbstractGeometryCodec<Box> {

    BoxCodec(ByteBufAllocator byteBufAllocator) {
        super(Box.class, BOX, byteBufAllocator);
    }

    @Override
    Box doDecodeBinary(ByteBuf buffer) {
        return Box.of(Point.of(buffer.readDouble(), buffer.readDouble()),
            Point.of(buffer.readDouble(), buffer.readDouble()));
    }

    @Override
    Box doDecodeText(String text) {
        TokenStream tokens = getTokenStream(text);
        return Box.of(Point.of(tokens.nextDouble(), tokens.nextDouble()),
            Point.of(tokens.nextDouble(), tokens.nextDouble()));
    }

    @Override
    ByteBuf doEncodeBinary(Box value) {
        return this.byteBufAllocator.buffer(32)
            .writeDouble(value.getA().getX())
            .writeDouble(value.getA().getY())
            .writeDouble(value.getB().getX())
            .writeDouble(value.getB().getY());
    }

    @Override
    public String encodeToText(Box value) {
        return value.toString();
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return BOX_ARRAY;
    }

}
