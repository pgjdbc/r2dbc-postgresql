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
import reactor.util.annotation.Nullable;

import java.net.URI;

final class UriCodec extends AbstractCodec<URI> {

    private final StringCodec delegate;

    UriCodec(ByteBufAllocator byteBufAllocator) {
        super(URI.class);

        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.delegate = new StringCodec(byteBufAllocator);
    }

    @Override
    public EncodedParameter encodeNull() {
        return this.delegate.encodeNull();
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.delegate.doCanDecode(type, format);
    }

    @Override
    URI doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends URI> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return URI.create(this.delegate.doDecode(buffer, dataType, format, String.class).trim());
    }

    @Override
    EncodedParameter doEncode(URI value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(value.toString());
    }

    @Override
    EncodedParameter doEncode(URI value, PostgresqlObjectId dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(value.toString(), dataType);
    }

}
