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
import reactor.util.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;

final class LocalDateTimeCodec extends AbstractCodec<LocalDateTime> {

    private final ByteBufAllocator byteBufAllocator;

    LocalDateTimeCodec(ByteBufAllocator byteBufAllocator) {
        super(LocalDateTime.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(TIMESTAMP, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return TIMESTAMP == type;
    }

    @Override
    LocalDateTime doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends LocalDateTime> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        if (format == FORMAT_BINARY) {
            return LocalDateTime.ofInstant(EpochTime.fromLong(buffer.readLong()).toInstant(), ZoneId.of("UTC"));
        }

        return PostgresqlDateTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(buffer), LocalDateTime::from);
    }

    @Override
    Parameter doEncode(LocalDateTime value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuf encoded = ByteBufUtils.encode(this.byteBufAllocator, value.toString());
        return create(TIMESTAMP, FORMAT_TEXT, Flux.just(encoded));
    }

}
