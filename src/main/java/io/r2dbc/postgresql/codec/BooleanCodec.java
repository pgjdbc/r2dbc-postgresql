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

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;

final class BooleanCodec extends BuiltinCodecSupport<Boolean> {

    BooleanCodec(ByteBufAllocator byteBufAllocator) {
        super(Boolean.class, byteBufAllocator, BOOL, BOOL_ARRAY, it -> it ? "t" : "f");
    }

    @Override
    Boolean doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends Boolean> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        if (format == Format.FORMAT_BINARY) {
            return buffer.readBoolean();
        }

        String decoded = ByteBufUtils.decode(buffer);
        return "1".equals(decoded)
            || "true".equalsIgnoreCase(decoded)
            || "t".equalsIgnoreCase(decoded)
            || "yes".equalsIgnoreCase(decoded)
            || "y".equalsIgnoreCase(decoded)
            || "on".equalsIgnoreCase(decoded);
    }

}
