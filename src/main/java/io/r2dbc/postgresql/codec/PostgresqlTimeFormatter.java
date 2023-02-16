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
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

class PostgresqlTimeFormatter {

    static final LocalTime MAX_TIME = LocalTime.MAX.minus(Duration.ofNanos(500));

    static final Duration ONE_MICROSECOND = Duration.ofNanos(1000);

    // pre-computed number representations
    static final char[][] NUMBERS;

    static final char[] ZEROS = {'0', '0', '0', '0', '0', '0', '0', '0', '0'};

    static {

        // The expected maximum value is 60 (seconds), so 64 is used "just in case"
        NUMBERS = new char[64][];
        for (int i = 0; i < NUMBERS.length; i++) {
            NUMBERS[i] = ((i < 10 ? "0" : "") + i).toCharArray();
        }
    }

    private static final DateTimeFormatter FULL_OFFSET =
        new DateTimeFormatterBuilder()
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
            .appendOffset("+HH:MM:ss", "+00:00")
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:mm", "+00:00")
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH", "+00")
            .optionalEnd()
            .toFormatter();


    /**
     * Parse {@link OffsetTime}.
     *
     * @param offsetTime backend representation.
     * @return the parsed {@link OffsetTime}.
     */
    static OffsetTime parseOffsetTime(String offsetTime) {

        if (offsetTime.startsWith("24:00:00")) {
            return OffsetTime.MAX;
        }

        return FULL_OFFSET.parse(offsetTime, OffsetTime::from);
    }

    /**
     * Render {@link OffsetTime} into the backend representation.
     *
     * @param offsetTime the value to render.
     * @return the rendered {@link OffsetTime}.
     */
    static String toString(OffsetTime offsetTime) {

        StringBuilder sbuf = new StringBuilder(8);

        LocalTime localTime = offsetTime.toLocalTime();
        if (localTime.isAfter(MAX_TIME)) {
            sbuf.append("24:00:00");
            appendTimeZone(sbuf, offsetTime.getOffset());
            return sbuf.toString();
        }

        int nano = offsetTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            offsetTime = offsetTime.plus(ONE_MICROSECOND);
        }
        appendTime(sbuf, localTime);
        appendTimeZone(sbuf, offsetTime.getOffset());

        return sbuf.toString();
    }


    /**
     * Parse {@link LocalTime}.
     *
     * @param localTime backend representation.
     * @return the parsed {@link LocalTime}.
     */
    static LocalTime parseLocalTime(String localTime) {

        if (localTime.equals("24:00:00")) {
            return LocalTime.MAX;
        }

        return LocalTime.parse(localTime);
    }

    /**
     * Render {@link LocalTime} into the backend representation.
     *
     * @param localTime the value to render.
     * @return the rendered {@link LocalTime}.
     */
    static String toString(LocalTime localTime) {

        StringBuilder sbuf = new StringBuilder(8);

        if (localTime.isAfter(MAX_TIME)) {
            return "24:00:00";
        }

        int nano = localTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            localTime = localTime.plus(ONE_MICROSECOND);
        }

        appendTime(sbuf, localTime);

        return sbuf.toString();
    }

    /**
     * Returns true when microsecond part of the time should be increased when rounding to microseconds.
     *
     * @param nanos nanosecond part of the time.
     * @return {@code true} when microsecond part of the time should be increased when rounding to microseconds.
     */
    static boolean nanosExceed499(int nanos) {
        return nanos % 1000 > 499;
    }

    static void appendTime(StringBuilder sb, LocalTime localTime) {
        appendTime(sb, localTime.getHour(), localTime.getMinute(), localTime.getSecond(), localTime.getNano());
    }

    static void appendTimeZone(StringBuilder sb, ZoneOffset offset) {
        appendTimeZone(sb, offset.getTotalSeconds());
    }

    private static void appendTimeZone(StringBuilder sb, int offset) {

        int absoff = Math.abs(offset);
        int hours = absoff / 60 / 60;
        int mins = (absoff - hours * 60 * 60) / 60;
        int secs = absoff - hours * 60 * 60 - mins * 60;

        sb.append((offset >= 0) ? "+" : "-");

        sb.append(NUMBERS[hours]);

        if (mins == 0 && secs == 0) {
            return;
        }
        sb.append(':');

        sb.append(NUMBERS[mins]);

        if (secs != 0) {
            sb.append(':');
            sb.append(NUMBERS[secs]);
        }
    }

    private static void appendTime(StringBuilder sb, int hours, int minutes, int seconds, int nanos) {

        sb.append(NUMBERS[hours]);

        sb.append(':');
        sb.append(NUMBERS[minutes]);

        sb.append(':');
        sb.append(NUMBERS[seconds]);

        // Add nanoseconds.
        // This won't work for server versions < 7.2 which only want
        // a two digit fractional second, but we don't need to support 7.1
        // anymore and getting the version number here is difficult.
        //
        if (nanos < 1000) {
            return;
        }
        sb.append('.');
        int len = sb.length();
        sb.append(nanos / 1000); // append microseconds
        int needZeros = 6 - (sb.length() - len);
        if (needZeros > 0) {
            sb.insert(len, ZEROS, 0, needZeros);
        }

        int end = sb.length() - 1;
        while (sb.charAt(end) == '0') {
            sb.deleteCharAt(end);
            end--;
        }
    }

}
