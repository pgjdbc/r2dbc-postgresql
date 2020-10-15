/*
 * Copyright 2019-2020 the original author or authors.
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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
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

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.DATE;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIME;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMETZ;

/**
 * Codec to decode all known temporal types.
 *
 * @param <T>
 */
abstract class AbstractTemporalCodec<T extends Temporal> extends AbstractCodec<T> {

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(DATE, TIMESTAMP, TIMESTAMPTZ, TIME, TIMETZ);

    /**
     * Create a new {@link AbstractTemporalCodec}.
     *
     * @param type the type handled by this codec
     */
    AbstractTemporalCodec(Class<T> type) {
        super(type);
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
    T decodeTemporal(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, Class<T> expectedType, Function<Temporal, T> converter) {
        Temporal number = decodeTemporal(buffer, dataType, format);
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
                if (FORMAT_BINARY == format) {
                    return EpochTime.fromLong(buffer.readLong()).toLocalDateTime();
                }

                return PostgresqlDateTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(buffer), LocalDateTime::from);
            case DATE:
                if (FORMAT_BINARY == format) {
                    return LocalDate.ofEpochDay(EpochTime.fromInt(buffer.readInt()).getJavaDays());
                }

                return LocalDate.parse(ByteBufUtils.decode(buffer));
            case TIME:
                if (FORMAT_BINARY == format) {
                    return LocalTime.ofNanoOfDay(buffer.readLong() * 1000);
                }

                return LocalTime.parse(ByteBufUtils.decode(buffer));
            case TIMESTAMPTZ:
                if (FORMAT_BINARY == format) {
                    return EpochTime.fromLong(buffer.readLong()).toInstant().atOffset(OffsetDateTime.now().getOffset());
                }

                return PostgresqlDateTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(buffer), ZonedDateTime::from);
            case TIMETZ:
                if (FORMAT_BINARY == format) {
                    long timeNano = buffer.readLong() * 1000;
                    int offsetSec = -buffer.readInt();
                    return OffsetTime.of(LocalTime.ofNanoOfDay(timeNano), ZoneOffset.ofTotalSeconds(offsetSec));
                }

                return PostgresqlTimeFormatter.INSTANCE.parse(ByteBufUtils.decode(buffer), OffsetTime::from);
        }

        throw new UnsupportedOperationException(String.format("Cannot decode value for type %s, format %s", dataType, format));
    }

    /**
     * Returns the {@link PostgresqlObjectId} for to identify whether this codec is the default codec.
     *
     * @return the {@link PostgresqlObjectId} for to identify whether this codec is the default codec
     */
    @Nullable
    abstract PostgresqlObjectId getDefaultType();

    static <T> T potentiallyConvert(Temporal temporal, Class<T> expectedType, Function<Temporal, T> converter) {
        return expectedType.isInstance(temporal) ? expectedType.cast(temporal) : converter.apply(temporal);
    }

}
