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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoEra;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.ERA;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR_OF_ERA;

class PostgresqlDateTimeFormatter {

    private static final String NEGATIVE_INFINITY = "-infinity";

    private static final String INFINITY = "infinity";

    // low value for dates is   4713 BC
    private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(4713, 1, 1).with(ChronoField.ERA, IsoEra.BCE.getValue());

    private static final LocalDateTime MIN_LOCAL_DATETIME = MIN_LOCAL_DATE.atStartOfDay();

    private static final OffsetDateTime MIN_OFFSET_DATETIME = MIN_LOCAL_DATETIME.atOffset(ZoneOffset.UTC);

    private static final LocalDate MAX_LOCAL_DATE = LocalDate.of(5874897, 12, 31);

    private static final OffsetDateTime MAX_OFFSET_DATETIME = OffsetDateTime.MAX.minus(Duration.ofMillis(500));

    private static final LocalDateTime MAX_LOCAL_DATETIME = LocalDateTime.MAX.minus(Duration.ofMillis(500));

    private static final DateTimeFormatter DATE_TIME_BACKEND =
        new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 10, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:MM:ss", "+00:00:00")
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:MM", "+00:00")
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH", "+00")
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, createMapping())
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter DATE_BACKEND =
        new DateTimeFormatterBuilder()
            .appendValue(YEAR_OF_ERA, 4, 10, SignStyle.NEVER)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .optionalStart()
            .appendLiteral(' ')
            .appendText(ERA, createMapping())
            .optionalEnd()
            .toFormatter();


    private static Map<Long, String> createMapping() {

        Map<Long, String> map = new HashMap<>();
        map.put(0L, "BC");
        map.put(1L, "AC");

        return map;
    }

    /**
     * Parse {@link LocalDate}.
     *
     * @param localDate backend representation.
     * @return the parsed {@link LocalDate}.
     */
    static LocalDate parseLocalDate(String localDate) {

        // convert postgres's infinity values to internal infinity magic value
        if (isInfinity(localDate)) {
            return LocalDate.MAX;
        }

        if (isNegativeInfinity(localDate)) {
            return LocalDate.MIN;
        }

        return DATE_BACKEND.parse(localDate, LocalDate::from);
    }

    /**
     * Render {@link LocalDate} into the backend representation.
     *
     * @param localDate the value to render.
     * @return the rendered {@link LocalDate}.
     */
    public static String toString(LocalDate localDate) {

        if (localDate.isAfter(MAX_LOCAL_DATE)) {
            return INFINITY;
        } else if (localDate.isBefore(MIN_LOCAL_DATE)) {
            return NEGATIVE_INFINITY;
        }

        StringBuilder sbuf = new StringBuilder(10);

        appendDate(sbuf, localDate);
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    /**
     * Parse {@link LocalDateTime}.
     *
     * @param localDateTime backend representation.
     * @return the parsed {@link LocalDateTime}.
     */
    static LocalDateTime parseLocalDateTime(String localDateTime) {

        // convert postgres's infinity values to internal infinity magic value
        if (isInfinity(localDateTime)) {
            return LocalDateTime.MAX;
        }

        if (isNegativeInfinity(localDateTime)) {
            return LocalDateTime.MIN;
        }

        return DATE_TIME_BACKEND.parse(localDateTime, LocalDateTime::from);
    }

    /**
     * Render {@link LocalDateTime} into the backend representation.
     *
     * @param localDateTime the value to render.
     * @return the rendered {@link LocalDateTime}.
     */
    static String toString(LocalDateTime localDateTime) {

        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return INFINITY;
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return NEGATIVE_INFINITY;
        }

        StringBuilder sbuf = new StringBuilder(24);

        int nano = localDateTime.getNano();
        if (PostgresqlTimeFormatter.nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            localDateTime = localDateTime.plus(PostgresqlTimeFormatter.ONE_MICROSECOND);
        }
        LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        PostgresqlTimeFormatter.appendTime(sbuf, localDateTime.toLocalTime());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    /**
     * Parse {@link OffsetDateTime}.
     *
     * @param offsetDateTime backend representation.
     * @return the parsed {@link OffsetDateTime}.
     */
    static OffsetDateTime parseOffsetDateTime(String offsetDateTime) {

        // convert postgres's infinity values to internal infinity magic value
        if (isInfinity(offsetDateTime)) {
            return OffsetDateTime.MAX;
        }

        if (isNegativeInfinity(offsetDateTime)) {
            return OffsetDateTime.MIN;
        }

        return DATE_TIME_BACKEND.parse(offsetDateTime, OffsetDateTime::from);
    }

    /**
     * Render {@link OffsetDateTime} into the backend representation.
     *
     * @param offsetDateTime the value to render.
     * @return the rendered {@link OffsetDateTime}.
     */
    static String toString(OffsetDateTime offsetDateTime) {

        if (offsetDateTime.isAfter(MAX_OFFSET_DATETIME)) {
            return INFINITY;
        } else if (offsetDateTime.isBefore(MIN_OFFSET_DATETIME)) {
            return NEGATIVE_INFINITY;
        }

        StringBuilder sbuf = new StringBuilder(24);

        int nano = offsetDateTime.getNano();
        if (PostgresqlTimeFormatter.nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            offsetDateTime = offsetDateTime.plus(PostgresqlTimeFormatter.ONE_MICROSECOND);
        }
        LocalDateTime localDateTime = offsetDateTime.toLocalDateTime();
        LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        PostgresqlTimeFormatter.appendTime(sbuf, localDateTime.toLocalTime());
        PostgresqlTimeFormatter.appendTimeZone(sbuf, offsetDateTime.getOffset());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    /**
     * Render {@link Instant} via {@link OffsetDateTime} into the backend representation.
     *
     * @param instant the value to render.
     * @return the rendered {@link Instant}.
     */
    static String toString(Instant instant) {

        if (instant.isAfter(MAX_OFFSET_DATETIME.toInstant())) {
            return INFINITY;
        } else if (instant.isBefore(MIN_OFFSET_DATETIME.toInstant())) {
            return NEGATIVE_INFINITY;
        }

        return toString(instant.atOffset(ZoneOffset.UTC));
    }

    private static void appendDate(StringBuilder sb, LocalDate localDate) {

        int year = localDate.get(ChronoField.YEAR_OF_ERA);
        int month = localDate.getMonthValue();
        int day = localDate.getDayOfMonth();
        appendDate(sb, year, month, day);
    }

    private static void appendDate(StringBuilder sb, int year, int month, int day) {

        // always use at least four digits for the year so very
        // early years, like 2, don't get misinterpreted

        int prevLength = sb.length();
        sb.append(year);
        int leadingZerosForYear = 4 - (sb.length() - prevLength);
        if (leadingZerosForYear > 0) {
            sb.insert(prevLength, PostgresqlTimeFormatter.ZEROS, 0, leadingZerosForYear);
        }

        sb.append('-');
        sb.append(PostgresqlTimeFormatter.NUMBERS[month]);
        sb.append('-');
        sb.append(PostgresqlTimeFormatter.NUMBERS[day]);
    }

    private static void appendEra(StringBuilder sb, LocalDate localDate) {
        if (localDate.get(ChronoField.ERA) == IsoEra.BCE.getValue()) {
            sb.append(" BC");
        }
    }

    private static boolean isNegativeInfinity(String localDateTime) {
        return localDateTime.length() == 9 && localDateTime.equals(NEGATIVE_INFINITY);
    }

    private static boolean isInfinity(String localDateTime) {
        return (localDateTime.length() == 8 && localDateTime.equals(INFINITY)) || localDateTime.length() == 9 && localDateTime.equals("+infinity");
    }

}
