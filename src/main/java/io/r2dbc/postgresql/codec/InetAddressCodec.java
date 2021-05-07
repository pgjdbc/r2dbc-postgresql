/*
 * Copyright 2017 the original author or authors.
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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INET;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INET_ARRAY;

final class InetAddressCodec extends BuiltinCodecSupport<InetAddress> {

    InetAddressCodec(ByteBufAllocator byteBufAllocator) {
        super(InetAddress.class, byteBufAllocator, INET, INET_ARRAY, InetAddress::getHostAddress);
    }

    @Override
    InetAddress doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends InetAddress> type) {
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

}
