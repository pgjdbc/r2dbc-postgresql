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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.core.publisher.Flux;
import java.time.ZoneId;
import reactor.util.annotation.Nullable;

import java.time.ZonedDateTime;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;

final class ZonedDateTimeCodec extends AbstractCodec<ZonedDateTime> {

    private final ByteBufAllocator byteBufAllocator;

    ZonedDateTimeCodec(ByteBufAllocator byteBufAllocator) {
        super(ZonedDateTime.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FORMAT_TEXT, TIMESTAMPTZ);
    }

    @Override
    boolean doCanDecode(Format format, PostgresqlObjectId type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return TIMESTAMPTZ == type;
    }

    @Override
    ZonedDateTime doDecode(ByteBuf byteBuf, @Nullable Format format, @Nullable Class<? extends ZonedDateTime> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");

        if (FORMAT_BINARY == format) {
            return EpochTime.fromLong(byteBuf.readLong()).toInstant().atZone(ZoneId.systemDefault());
        }

        return PostgresqlDateTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(byteBuf), ZonedDateTime::from);
    }

    @Override
    Parameter doEncode(ZonedDateTime value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuf encoded = ByteBufUtils.encode(this.byteBufAllocator, value.toOffsetDateTime().toString());
        return create(FORMAT_TEXT, TIMESTAMPTZ, Flux.just(encoded));
    }

}
