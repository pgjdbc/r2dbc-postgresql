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
import reactor.util.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP_ARRAY;

final class LocalDateTimeCodec extends AbstractTemporalCodec<LocalDateTime> {

    private final Supplier<ZoneId> zoneIdSupplier;

    LocalDateTimeCodec(ByteBufAllocator byteBufAllocator, Supplier<ZoneId> zoneIdSupplier) {
        super(LocalDateTime.class, byteBufAllocator, TIMESTAMP, TIMESTAMP_ARRAY, PostgresqlDateTimeFormatter::toString);
        this.zoneIdSupplier = Assert.requireNonNull(zoneIdSupplier, "zoneIdSupplier must not be null");
    }

    @Override
    LocalDateTime doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends LocalDateTime> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return decodeTemporal(buffer, dataType, format, LocalDateTime.class, temporal -> {
            ZoneId zone = this.zoneIdSupplier.get();

            if (temporal instanceof LocalDate) {
                return ((LocalDate) temporal).atStartOfDay(zone).toLocalDateTime();
            }

            Instant instant = Instant.from(temporal);
            ZoneOffset offset = zone.getRules().getOffset(instant);
            return instant.atOffset(offset).toLocalDateTime();
        });
    }

}
