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

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CIRCLE;

final class CircleCodec extends AbstractCodec<Circle> {

    private final ByteBufAllocator byteBufAllocator;

    CircleCodec(ByteBufAllocator byteBufAllocator) {
        super(Circle.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return CIRCLE == type;
    }

    @Override
    Circle doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends Circle> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");

        if (format == FORMAT_BINARY) {
            double x = buffer.readDouble();
            double y = buffer.readDouble();
            double r = buffer.readDouble();
            return new Circle(Point.of(x, y), r);
        }

        String decodedAsString = ByteBufUtils.decode(buffer);
        String parenRemovedVal = decodedAsString.replaceAll("[()<>]", "");
        String[] coordinatesAsString = parenRemovedVal.split(",");
        double x = Double.parseDouble(coordinatesAsString[0]);
        double y = Double.parseDouble(coordinatesAsString[1]);
        double r = Double.parseDouble(coordinatesAsString[2]);
        return new Circle(Point.of(x, y), r);
    }

    /**
     * @param value the  {@code value}.
     * @return Circle in string format as understood by Postgresql - &lt(x,y),r&gt
     */
    @Override
    Parameter doEncode(Circle value) {
        Assert.requireNonNull(value, "value must not be null");
        Point center = value.getCenter();
        return create(CIRCLE, FORMAT_BINARY, () -> this.byteBufAllocator.buffer(lengthInBytes())
            .writeDouble(center.getX())
            .writeDouble(center.getY())
            .writeDouble(value.getRadius()));
    }

    @Override
    public Parameter encodeNull() {
        return createNull(CIRCLE, FORMAT_BINARY);
    }

    int lengthInBytes() {
        return 24;
    }

}
