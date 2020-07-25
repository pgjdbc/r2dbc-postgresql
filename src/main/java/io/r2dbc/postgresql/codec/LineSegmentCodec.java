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
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.LSEG;

final class LineSegmentCodec extends AbstractCodec<LineSegment> {

    private final ByteBufAllocator byteBufAllocator;

    LineSegmentCodec(ByteBufAllocator byteBufAllocator) {
        super(LineSegment.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, @Nullable Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return LSEG == type;
    }

    @Override
    LineSegment doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends LineSegment> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");

        if (format == FORMAT_BINARY) {
            double x1 = buffer.readDouble();
            double y1 = buffer.readDouble();
            double x2 = buffer.readDouble();
            double y2 = buffer.readDouble();
            return new LineSegment(Point.of(x1, y1), Point.of(x2, y2));
        }

        String decodedAsString = ByteBufUtils.decode(buffer);
        String parenRemovedVal = decodedAsString.replaceAll("[()]", "")
            .replace("[", "")
            .replace("]", "")
            .trim();
        String[] coordinatesAsString = parenRemovedVal.split(",");
        double x1 = Double.parseDouble(coordinatesAsString[0]);
        double y1 = Double.parseDouble(coordinatesAsString[1]);
        double x2 = Double.parseDouble(coordinatesAsString[2]);
        double y2 = Double.parseDouble(coordinatesAsString[3]);
        return new LineSegment(Point.of(x1, y1), Point.of(x2, y2));
    }

    @Override
    Parameter doEncode(LineSegment value) {
        Assert.requireNonNull(value, "value must not be null");
        Point p1 = value.getPoint1();
        Point p2 = value.getPoint2();
        return create(LSEG, FORMAT_BINARY, () -> this.byteBufAllocator
            .buffer(lengthInBytes())
            .writeDouble(p1.getX())
            .writeDouble(p1.getY())
            .writeDouble(p2.getX())
            .writeDouble(p2.getY()));
    }

    @Override
    public Parameter encodeNull() {
        return createNull(LSEG, FORMAT_BINARY);
    }

    int lengthInBytes() {
        return 32;
    }

}
