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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME_ARRAY;

final class LocalTimeCodec extends AbstractTemporalCodec<LocalTime> {

    LocalTimeCodec(ByteBufAllocator byteBufAllocator) {
        super(LocalTime.class, byteBufAllocator, TIME, TIME_ARRAY, PostgresqlTimeFormatter::toString);
    }

    @Override
    LocalTime doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, Class<? extends LocalTime> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return decodeTemporal(buffer, dataType, format, LocalTime.class, temporal -> {

            if (temporal instanceof LocalDateTime) {
                return ((LocalDateTime) temporal).toLocalTime();
            }
            return Instant.from(temporal).atOffset(ZoneOffset.UTC).toLocalTime();
        });
    }

}
