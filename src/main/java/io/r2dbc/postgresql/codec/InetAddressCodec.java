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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INET;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class InetAddressCodec extends AbstractCodec<InetAddress> {

    private final ByteBufAllocator byteBufAllocator;

    InetAddressCodec(ByteBufAllocator byteBufAllocator) {
        super(InetAddress.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(FORMAT_TEXT, INET);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return INET == type;
    }

    @Override
    InetAddress doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends InetAddress> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        try {
            if (format == Format.FORMAT_BINARY) {

                int readableBytes = buffer.readableBytes();
                if (readableBytes == 8) {
                    // addr + cidr
                    buffer.skipBytes(4);
                    byte[] addr = new byte[4];
                    buffer.readBytes(addr);
                    return InetAddress.getByAddress(addr);
                }

                throw new IllegalArgumentException("Cannot decode InetAddress. Available bytes: " + readableBytes);
            }

            return InetAddress.getByName(ByteBufUtils.decode(buffer));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    EncodedParameter doEncode(InetAddress value) {
        return doEncode(value, INET);
    }

    @Override
    EncodedParameter doEncode(InetAddress value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(FORMAT_TEXT, dataType, () -> ByteBufUtils.encode(this.byteBufAllocator, value.getHostAddress()));
    }

}
