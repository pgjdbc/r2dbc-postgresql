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
import io.r2dbc.postgresql.type.PostgresqlObjectId;

final class LineCodec extends AbstractGeometryCodec<Line> {

    LineCodec(ByteBufAllocator byteBufAllocator) {
        super(Line.class, PostgresqlObjectId.LINE, byteBufAllocator);
    }

    @Override
    Line doDecodeBinary(ByteBuf byteBuffer) {
        return Line.of(byteBuffer.readDouble(), byteBuffer.readDouble(), byteBuffer.readDouble());
    }

    @Override
    Line doDecodeText(String text) {
        TokenStream stream = getTokenStream(text);
        return Line.of(stream.nextDouble(), stream.nextDouble(), stream.nextDouble());
    }

    @Override
    ByteBuf doEncodeBinary(Line value) {
        return this.byteBufAllocator
            .buffer(24)
            .writeDouble(value.getA())
            .writeDouble(value.getB())
            .writeDouble(value.getC());
    }

}
