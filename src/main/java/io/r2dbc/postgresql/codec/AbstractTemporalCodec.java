/*
 * Copyright 2019 the original author or authors.
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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.DATE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMETZ;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

/**
 * Codec to decode all known temporal types.
 *
 * @param <T> the type that is handled by this {@link Codec}
 */
abstract class AbstractTemporalCodec<T extends Temporal> extends BuiltinCodecSupport<T> {

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(DATE, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ);

    private final PostgresqlObjectId postgresType;

    /**
     * Create a new {@link AbstractTemporalCodec}.
     *
     * @param type the type handled by this codec
     */
    AbstractTemporalCodec(Class<T> type, ByteBufAllocator byteBufAllocator, PostgresqlObjectId postgresType, PostgresqlObjectId postgresArrayType, Function<T, String> toTextEncoder) {
        super(type, byteBufAllocator, postgresType, postgresArrayType, toTextEncoder);
        this.postgresType = postgresType;
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (type == Object.class) {
            if (PostgresqlObjectId.isValid(dataType) && PostgresqlObjectId.valueOf(dataType) != getDefaultType()) {
                return false;
            }
        }
        return super.canDecode(dataType, format, type);
    }

    @Override
    final boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return SUPPORTED_TYPES.contains(type);
    }

    /**
     * Decode {@code buffer} to {@link Temporal} and potentially convert it to {@link Class expectedType} using {@link Function converter} if the decoded type does not match {@code expectedType}.
     *
     * @param buffer       the buffer
     * @param dataType     the Postgres type
     * @param format       value format
     * @param expectedType expected result type
     * @param converter    converter to convert from {@link Temporal} into {@code expectedType}
     * @return the decoded value
     */
    T decodeTemporal(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, Class<T> expectedType, Function<Temporal, T> converter) {
        Temporal number = decodeTemporal(buffer, PostgresqlObjectId.from(dataType), format);
        return potentiallyConvert(number, expectedType, converter);
    }

    /**
     * Decode {@code buffer} to {@link Temporal} according to {@link PostgresqlObjectId}.
     *
     * @param buffer   the buffer
     * @param dataType the Postgres type
     * @param format   value format
     * @return the decoded value
     */
    private Temporal decodeTemporal(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        switch (dataType) {

            case TIMESTAMP:
            case TIMESTAMP_ARRAY:
                if (FORMAT_BINARY == format) {
                    return EpochTime.fromLong(buffer.readLong()).toLocalDateTime();
                }

                return PostgresqlDateTimeFormatter.parse(ByteBufUtils.decode(buffer), LocalDateTime::from);
            case DATE:
            case DATE_ARRAY:
                if (FORMAT_BINARY == format) {
                    return LocalDate.ofEpochDay(EpochTime.fromInt(buffer.readInt()).getJavaDays());
                }

                return LocalDate.parse(ByteBufUtils.decode(buffer));
            case TIME:
            case TIME_ARRAY:
                if (FORMAT_BINARY == format) {
                    return LocalTime.ofNanoOfDay(buffer.readLong() * 1000);
                }

                return LocalTime.parse(ByteBufUtils.decode(buffer));
            case TIMESTAMPTZ:
            case TIMESTAMPTZ_ARRAY:
                if (FORMAT_BINARY == format) {
                    return EpochTime.fromLong(buffer.readLong()).toInstant().atOffset(OffsetDateTime.now().getOffset());
                }

                return PostgresqlDateTimeFormatter.parse(ByteBufUtils.decode(buffer), ZonedDateTime::from);
            case TIMETZ:
            case TIMETZ_ARRAY:
                if (FORMAT_BINARY == format) {
                    long timeNano = buffer.readLong() * 1000;
                    int offsetSec = -buffer.readInt();
                    return OffsetTime.of(LocalTime.ofNanoOfDay(timeNano), ZoneOffset.ofTotalSeconds(offsetSec));
                }

                return PostgresqlTimeFormatter.parse(ByteBufUtils.decode(buffer), OffsetTime::from);
        }

        throw new UnsupportedOperationException(String.format("Cannot decode value for type %s, format %s", dataType, format));
    }

    /**
     * Returns the {@link PostgresqlObjectId} for to identify whether this codec is the default codec.
     *
     * @return the {@link PostgresqlObjectId} for to identify whether this codec is the default codec
     */
    @Nullable
    PostgresqlObjectId getDefaultType() {
        return this.postgresType;
    }

    static <T> T potentiallyConvert(Temporal temporal, Class<T> expectedType, Function<Temporal, T> converter) {
        return expectedType.isInstance(temporal) ? expectedType.cast(temporal) : converter.apply(temporal);
    }

}
