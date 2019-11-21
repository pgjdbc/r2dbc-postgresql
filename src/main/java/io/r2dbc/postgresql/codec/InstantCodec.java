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
import reactor.util.annotation.Nullable;

import java.time.*;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;

final class InstantCodec extends AbstractTemporalCodec<Instant> {

    private final ByteBufAllocator byteBufAllocator;

    InstantCodec(ByteBufAllocator byteBufAllocator) {
        super(Instant.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(TIMESTAMPTZ, FORMAT_TEXT);
    }

    @Override
    Instant doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, Class<? extends Instant> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return decodeTemporal(buffer, dataType, format, Instant.class, temporal -> {

            if (temporal instanceof LocalDateTime) {
                return ((LocalDateTime) temporal).toInstant(ZoneOffset.UTC);
            }

            if (temporal instanceof LocalDate) {
                return ((LocalDate) temporal).atStartOfDay(ZoneId.systemDefault()).toInstant();
            }

            return Instant.from(temporal);
        });
    }

    @Override
    Parameter doEncode(Instant value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(TIMESTAMPTZ, FORMAT_TEXT, () -> ByteBufUtils.encode(this.byteBufAllocator, value.toString()));
    }

    @Override
    PostgresqlObjectId getDefaultType() {
        return null;
    }
}
