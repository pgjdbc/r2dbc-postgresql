/*
 * Copyright 2019-2020 the original author or authors.
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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.R2dbcNonTransientResourceException;

import java.io.IOException;
import java.io.InputStream;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;

final class JsonInputStreamCodec extends AbstractJsonCodec<InputStream> {

    private final ByteBufAllocator byteBufAllocator;

    JsonInputStreamCodec(ByteBufAllocator byteBufAllocator) {
        super(InputStream.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    InputStream doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends InputStream> type) {
        return new ByteBufInputStream(buffer.retain().touch("Decoded by JsonInputStreamCodec"), true);
    }

    @Override
    EncodedParameter doEncode(InputStream value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(JSONB, FORMAT_BINARY, () -> doEncode(value, this.byteBufAllocator));
    }

    static ByteBuf doEncode(InputStream value, ByteBufAllocator alloc) {

        try (InputStream in = value) {

            ByteBuf json = alloc.buffer();

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                json.writeBytes(buffer, 0, bytesRead);
            }

            return Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(new byte[]{1}), json);
        } catch (IOException e) {
            throw new R2dbcNonTransientResourceException("Cannot write InputStream", e);
        }
    }

}
