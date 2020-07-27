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

import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;

final class PointCodec extends AbstractGeometryCodec<Point> {

    PointCodec(ByteBufAllocator byteBufAllocator) {
        super(Point.class, POINT, byteBufAllocator);
    }

    @Override
    Point doDecodeBinary(ByteBuf byteBuffer) {
        double x = byteBuffer.readDouble();
        double y = byteBuffer.readDouble();
        return Point.of(x, y);
    }

    @Override
    Point doDecodeText(String text) {
        List<String> tokens = tokenizeTextData(text);
        double x = Double.parseDouble(tokens.get(0));
        double y = Double.parseDouble(tokens.get(1));
        return Point.of(x, y);
    }

    @Override
    ByteBuf doEncodeBinary(Point value) {
        return this.byteBufAllocator.buffer(lengthInBytes())
            .writeDouble(value.getX()).writeDouble(value.getY());
    }

    int lengthInBytes() {
        return 16;
    }

}
