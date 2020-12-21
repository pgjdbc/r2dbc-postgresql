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
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NAME;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;

final class StringCodec extends AbstractCodec<String> {

    private final ByteBufAllocator byteBufAllocator;

    StringCodec(ByteBufAllocator byteBufAllocator) {
        super(String.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(FORMAT_TEXT, VARCHAR);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return BPCHAR == type || CHAR == type || TEXT == type || UNKNOWN == type || VARCHAR == type || NAME == type;
    }

    @Override
    String doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends String> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return ByteBufUtils.decode(buffer);
    }

    @Override
    EncodedParameter doEncode(String value) {
        return doEncode(value, VARCHAR);
    }

    @Override
    EncodedParameter doEncode(String value, PostgresqlObjectId dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(FORMAT_TEXT, dataType, () -> ByteBufUtils.encode(this.byteBufAllocator, value));
    }

}
