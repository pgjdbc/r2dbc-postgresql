/*
 * Copyright 2017-2020 the original author or authors.
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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;

public class PointCodec extends AbstractCodec<Point> {

    private final ByteBufAllocator byteBufAllocator;

    PointCodec(ByteBufAllocator byteBufAllocator) {
        super(Point.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, @Nullable Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return POINT == type;
    }

    @Override
    Point doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends Point> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(format, "format must not be null");

        if (format == FORMAT_BINARY) {
            double x = buffer.readDouble();
            double y = buffer.readDouble();
            return new Point(x, y);
        } else {
            String decodedAsString = ByteBufUtils.decode(buffer);
            String parenRemovedVal = decodedAsString.replaceAll("[()]", "");
            String[] coordinatesAsString = parenRemovedVal.split(",");
            double x = Double.parseDouble(coordinatesAsString[0]);
            double y = Double.parseDouble(coordinatesAsString[1]);
            return new Point(x, y);
        }
    }

    /**
     * @param value the  {@code value}.
     * @return Point in String format as understood by postgresql - (x,y)
     */
    @Override
    Parameter doEncode(Point value) {
        Assert.requireNonNull(value, "value must not be null");
        return create(POINT, FORMAT_BINARY, () -> this.byteBufAllocator.buffer(lengthInBytes()).writeDouble(value.getX()).writeDouble(value.getY()));
    }

    @Override
    public Parameter encodeNull() {
        return createNull(POINT, FORMAT_BINARY);
    }

    public int lengthInBytes() {
        return 16;
    }

}
