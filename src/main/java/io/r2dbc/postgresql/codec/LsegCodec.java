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

/**
 * @since 0.8.5
 */
final class LsegCodec extends AbstractGeometryCodec<Lseg> {

    LsegCodec(ByteBufAllocator byteBufAllocator) {
        super(Lseg.class, PostgresqlObjectId.LSEG, byteBufAllocator);
    }

    @Override
    Lseg doDecodeBinary(ByteBuf byteBuffer) {
        return Lseg.of(Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()),
            Point.of(byteBuffer.readDouble(), byteBuffer.readDouble()));
    }

    @Override
    Lseg doDecodeText(String text) {
        TokenStream stream = getTokenStream(text);
        return Lseg.of(Point.of(stream.nextDouble(), stream.nextDouble()),
            Point.of(stream.nextDouble(), stream.nextDouble()));
    }

    @Override
    ByteBuf doEncodeBinary(Lseg value) {
        return this.byteBufAllocator
            .buffer(32)
            .writeDouble(value.getP1().getX())
            .writeDouble(value.getP1().getY())
            .writeDouble(value.getP2().getX())
            .writeDouble(value.getP2().getY());
    }

}
