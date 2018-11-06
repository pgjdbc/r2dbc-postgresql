/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;

final class InstantCodec extends AbstractCodec<Instant> {

    private final ByteBufAllocator byteBufAllocator;

    InstantCodec(ByteBufAllocator byteBufAllocator) {
        super(Instant.class);
        this.byteBufAllocator = Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FORMAT_TEXT, TIMESTAMP);
    }

    @Override
    boolean doCanDecode(Format format, PostgresqlObjectId type) {
        Objects.requireNonNull(format, "format must not be null");
        Objects.requireNonNull(type, "type must not be null");

        return FORMAT_TEXT == format && TIMESTAMP == type;
    }

    @Override
    Instant doDecode(ByteBuf byteBuf, @Nullable Format format, @Nullable Class<? extends Instant> type) {
        Objects.requireNonNull(byteBuf, "byteBuf must not be null");
        return PostgresqlDateTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(byteBuf), temporal -> ZonedDateTime.of(LocalDateTime.from(temporal), ZoneOffset.UTC).toInstant());
    }

    @Override
    Parameter doEncode(Instant value) {
        Objects.requireNonNull(value, "value must not be null");

        ByteBuf encoded = ByteBufUtils.encode(this.byteBufAllocator, value.toString());
        return create(FORMAT_TEXT, TIMESTAMP, encoded);
    }

}
