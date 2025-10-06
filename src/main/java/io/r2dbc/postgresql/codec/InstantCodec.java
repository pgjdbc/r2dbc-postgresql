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
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ_ARRAY;

final class InstantCodec extends AbstractTemporalCodec<Instant> {

    private final Supplier<ZoneId> zoneIdSupplier;

    InstantCodec(ByteBufAllocator byteBufAllocator, Supplier<ZoneId> zoneIdSupplier) {
        super(Instant.class, byteBufAllocator, TIMESTAMPTZ, TIMESTAMPTZ_ARRAY, it -> PostgresqlDateTimeFormatter.toString(it));
        this.zoneIdSupplier = zoneIdSupplier;
    }

    @Override
    Instant doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, Class<? extends Instant> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return decodeTemporal(buffer, dataType, format, Instant.class, temporal -> {

            if (temporal instanceof LocalDateTime) {
                ZoneId zoneId = this.zoneIdSupplier.get();
                ZoneOffset offset = zoneId.getRules().getOffset((LocalDateTime) temporal);
                return ((LocalDateTime) temporal).toInstant(offset);
            }

            if (temporal instanceof LocalDate) {
                ZoneId zoneId = this.zoneIdSupplier.get();

                return ((LocalDate) temporal).atStartOfDay(zoneId).toInstant();
            }

            return Instant.from(temporal);
        });
    }

    // Avoid defaulting, use OffsetDateTime as default instead.
    @Override
    PostgresqlObjectId getDefaultType() {
        return null;
    }

}
