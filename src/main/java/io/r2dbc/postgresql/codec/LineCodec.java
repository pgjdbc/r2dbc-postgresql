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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE_ARRAY;

/**
 * @since 0.8.5
 */
final class LineCodec extends AbstractGeometryCodec<Line> {

    LineCodec(ByteBufAllocator byteBufAllocator) {
        super(Line.class, LINE, byteBufAllocator);
    }

    @Override
    Line doDecodeBinary(ByteBuf byteBuffer) {
        return Line.of(byteBuffer.readDouble(), byteBuffer.readDouble(), byteBuffer.readDouble());
    }

    /**
     * Decodes {@code text} into lines that are represented by the linear
     * equation Ax + By + C = 0, where A and B are not both zero.
     * <p>
     * Values of type line are input and output in the following form:
     * <ul>
     *     <li>{ A, B, C }</li>
     * </ul>
     * <p>
     * Alternatively, any of the following forms can be used for input:
     * <ul>
     *     <li>[ ( x1 , y1 ) , ( x2 , y2 ) ]</li>
     *     <li>( ( x1 , y1 ) , ( x2 , y2 ) )</li>
     *     <li>&nbsp; ( x1 , y1 ) , ( x2 , y2 )</li>
     *     <li>&nbsp; &nbsp; x1 , y1 &nbsp; , &nbsp; x2 , y2</li>
     * </ul>
     *
     * @param text string containing the textual representation of the value to decode
     * @return the decoded line
     */
    @Override
    Line doDecodeText(String text) {
        List<Double> list = new ArrayList<>();
        getTokenStream(text).forEachRemaining(e -> list.add(Double.parseDouble(e)));
        return list.size() == 3
            ? Line.of(list.get(0), list.get(1), list.get(2))
            : Line.of(list.get(0), list.get(1), list.get(2), list.get(3));
    }

    @Override
    ByteBuf doEncodeBinary(Line value) {
        return this.byteBufAllocator
            .buffer(24)
            .writeDouble(value.getA())
            .writeDouble(value.getB())
            .writeDouble(value.getC());
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return LINE_ARRAY;
    }

}
