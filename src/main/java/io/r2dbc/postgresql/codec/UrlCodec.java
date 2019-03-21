/*
 * Copyright 2017-2019 the original author or authors.
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
import reactor.util.annotation.Nullable;

import java.net.MalformedURLException;
import java.net.URL;

final class UrlCodec extends AbstractCodec<URL> {

    private final StringCodec delegate;

    UrlCodec(ByteBufAllocator byteBufAllocator) {
        super(URL.class);

        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.delegate = new StringCodec(byteBufAllocator);
    }

    @Override
    public Parameter doEncode(URL value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(value.toString());
    }

    @Override
    public Parameter encodeNull() {
        return this.delegate.encodeNull();
    }

    @Override
    boolean doCanDecode(Format format, PostgresqlObjectId type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.delegate.doCanDecode(format, type);
    }

    @Override
    URL doDecode(ByteBuf byteBuf, @Nullable Format format, @Nullable Class<? extends URL> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");

        try {
            return new URL(this.delegate.doDecode(byteBuf, format, String.class).trim());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
