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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.REF_CURSOR;

final class RefCursorNameCodec extends AbstractCodec<String> {

    static final RefCursorNameCodec INSTANCE = new RefCursorNameCodec();

    RefCursorNameCodec() {
        super(String.class);
    }

    @Override
    public EncodedParameter encodeNull() {
        throw new UnsupportedOperationException("Cannot encode RefCursor");
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return REF_CURSOR == type;
    }

    @Override
    String doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends String> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return ByteBufUtils.decode(buffer);
    }

    @Override
    EncodedParameter doEncode(String value) {
        throw new UnsupportedOperationException("Cannot encode RefCursor");
    }

    @Override
    EncodedParameter doEncode(String value, PostgresTypeIdentifier dataType) {
        throw new UnsupportedOperationException("Cannot encode RefCursor");
    }

}
