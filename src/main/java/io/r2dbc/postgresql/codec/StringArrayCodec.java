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

import java.util.function.Supplier;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BPCHAR_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CHAR_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NAME_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TEXT_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class StringArrayCodec extends AbstractArrayCodec<String> {

    StringArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, String.class, TEXT_ARRAY);
    }

    @Override
    String doDecodeBinary(ByteBuf byteBuffer) {
        return ByteBufUtils.decode(byteBuffer);
    }

    @Override
    String doDecodeText(String text) {
        return text;
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return (BPCHAR_ARRAY == type || CHAR_ARRAY == type || TEXT_ARRAY == type || VARCHAR_ARRAY == type | NAME_ARRAY == type);
    }

    @Override
    EncodedParameter encodeArray(Supplier<ByteBuf> encodedSupplier, PostgresTypeIdentifier dataType) {
        return create(FORMAT_TEXT, dataType, encodedSupplier);
    }

    @Override
    String doEncodeText(String value) {
        Assert.requireNonNull(value, "value must not be null");

        return AbstractArrayCodec.escapeArrayElement(value);
    }

}
