/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.postgresql.util.Assert;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Period;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Value object that maps to the {@code interval} datatype in Postgres.
 * <p>
 * Uses {@code Duration} to represent the interval.
 *
 * @since 0.9.0
 */
public final class Interval {

    /**
     * Constant for a duration of zero.
     */
    public static final Interval ZERO = new Interval(0, 0, 0, 0, 0, 0, 0);

    /**
     * Number of microseconds in one second
     */
    private static final int MICROSECONDS_IN_SECOND = 1000000;

    /**
     * Number of seconds in one minute
     */
    private static final int SECONDS_IN_MINUTE = 60;

    /**
     * Number of seconds in one hour
     */
    private static final int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;

    /**
     * Number of seconds in one day
     */
    private static final int SECONDS_IN_DAY = SECONDS_IN_HOUR * 24;

    /**
     * Number of months in one year
     */
    private static final int MONTHS_IN_YEAR = 12;

    private final int years;
    private final int months;
    private final int days;
    private final int hours;
    private final int minutes;
    private final int seconds;
    private final int microseconds;

    private Interval(Period period) {
        this(period, Duration.ZERO);
    }

    private Interval(Duration duration) {
        this(Period.ZERO, duration);
    }

    private Interval(Period period, Duration duration) {
        Assert.requireNonNull(period, "period must not be null");
        Assert.requireNonNull(duration, "duration must not be null");

        long remainderSeconds = duration.getSeconds();

        this.years = period.getYears() + (period.getMonths() / MONTHS_IN_YEAR);
        this.months = period.getMonths() % MONTHS_IN_YEAR;
        this.days = period.getDays() + (int) (remainderSeconds / SECONDS_IN_DAY);
        remainderSeconds = remainderSeconds % SECONDS_IN_DAY;
        this.hours = (int) remainderSeconds / SECONDS_IN_HOUR;
        remainderSeconds = remainderSeconds % SECONDS_IN_HOUR;
        this.minutes = (int) remainderSeconds / SECONDS_IN_MINUTE;
        this.seconds = (int) remainderSeconds % SECONDS_IN_MINUTE;
        this.microseconds = (int) TimeUnit.NANOSECONDS.toMicros(duration.getNano());
    }

    private Interval(int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {
        this.years = years;
        this.months = months;
        this.days = days;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
        this.microseconds = microseconds;
    }

    /**
     * Create a new {@link Interval} given a {@link Period period} object.
     *
     * @param period the period object
     * @return the new {@link Interval} object
     * @throws IllegalArgumentException if {@code period} is {@code null}
     */
    public static Interval of(Period period) {
        if (Period.ZERO.equals(period)) {
            return Interval.ZERO;
        }
        return new Interval(period);
    }

    /**
     * Create a new {@link Interval} given a {@link Duration duration} object.
     *
     * @param duration the duration object
     * @return the new {@link Interval} object
     * @throws IllegalArgumentException if {@code duration} is {@code null}
     */
    public static Interval of(Duration duration) {
        if (Duration.ZERO.equals(duration)) {
            return Interval.ZERO;
        }
        return new Interval(duration);
    }

    /**
     * Create a new {@link Interval} given a {@link Period period} and
     * {@link Duration duration} object.
     *
     * @param period   the period object
     * @param duration the duration object
     * @return the new {@link Interval} object
     * @throws IllegalArgumentException if {@code period} or {@code duration} is {@code null}
     */
    public static Interval of(Period period, Duration duration) {
        if (Period.ZERO.equals(period) && Duration.ZERO.equals(duration)) {
            return Interval.ZERO;
        }
        return new Interval(period, duration);
    }

    /**
     * Create a new {@link Interval} given the {@code years}, {@code months},
     * {@code days}, {@code hours}, {@code minutes}, {@code seconds},
     * and {@code microseconds}.
     *
     * @param years        interval years
     * @param months       interval months
     * @param days         interval days
     * @param hours        interval hours
     * @param minutes      interval minutes
     * @param seconds      interval seconds
     * @param microseconds interval microseconds
     * @return the new {@link Interval} object
     */
    public static Interval of(int years, int months, int days, int hours, int minutes, int seconds, int microseconds) {
        if ((years | months | days | hours | minutes | seconds | microseconds) == 0) {
            return ZERO;
        }
        return new Interval(years, months, days, hours, minutes, seconds, microseconds);
    }

    /**
     * Parses the {@code value} representing a {@link Interval}. This method
     * only supports values that Postgres returns.
     *
     * @param value a string that represents the interval value
     * @return the new {@link Interval} object
     */
    public static Interval parse(String value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.startsWith("P") ?
                parseISO8601IntervalValue(value) :
                parsePostgresIntervalValue(value);
    }

    /**
     * Returns the years represented by this interval.
     *
     * @return the years represented by this interval
     */
    public int getYears() {
        return years;
    }

    /**
     * Returns the months represented by this interval.
     *
     * @return the months represented by this interval
     */
    public int getMonths() {
        return months;
    }

    /**
     * Returns the days represented by this interval.
     *
     * @return the days represented by this interval
     */
    public int getDays() {
        return days;
    }

    /**
     * Returns the hours represented by this interval.
     *
     * @return the hours represented by this interval
     */
    public int getHours() {
        return hours;
    }

    /**
     * Returns the hours represented by this interval.
     *
     * @return the hours represented by this interval
     */
    public int getMinutes() {
        return minutes;
    }

    /**
     * Returns the seconds represented by this interval.
     *
     * @return seconds represented by this interval
     */
    public double getSeconds() {
        return seconds + (double) microseconds / MICROSECONDS_IN_SECOND;
    }

    /**
     * Returns a string representing the interval value.
     *
     * @return a string representing the interval value
     */
    public String getValue() {
        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        return new StringBuilder(64)
                .append(years).append(" yr ")
                .append(months).append(" mon ")
                .append(days).append(" day ")
                .append(hours).append(" hr ")
                .append(minutes).append(" min ")
                .append(df.format(getSeconds()))
                .append(" sec").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Interval interval = (Interval) o;
        return years == interval.years &&
                months == interval.months &&
                days == interval.days &&
                hours == interval.hours &&
                minutes == interval.minutes &&
                seconds == interval.seconds &&
                microseconds == interval.microseconds;
    }

    @Override
    public int hashCode() {
        return (((((((8 * 31 + microseconds) * 31 + seconds) * 31 + minutes) * 31 + hours) * 31
                + days) * 31 + months) * 31 + years) * 31;
    }

    /**
     * Parses the ISO8601 {@code value} representing an {@link Interval}.
     *
     * @param value the value in ISO6501 format
     * @return the new {@link Interval} object
     */
    private static Interval parseISO8601IntervalValue(String value) {

        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        boolean timeFlag = false, microsecondFlag = false, negativeFlag = false;
        int currentValue = 0;
        int multiplier = 1;

        for (int i = 1; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == 'T') {
                timeFlag = true;
            } else if (c == '-') {
                negativeFlag = true;
            } else if (Character.isDigit(c)) {
                currentValue = (currentValue * multiplier) + (c - '0');
                multiplier = 10;
            } else {

                if (c == 'Y') {
                    years = getValue(currentValue, negativeFlag);
                } else if (!timeFlag && c == 'M') {
                    months = getValue(currentValue, negativeFlag);
                } else if (c == 'D') {
                    days = getValue(currentValue, negativeFlag);
                } else if (c == 'H') {
                    hours = getValue(currentValue, negativeFlag);
                } else if (timeFlag && c == 'M') {
                    minutes = getValue(currentValue, negativeFlag);
                } else if (!microsecondFlag && (c == 'S' || c == '.')) {
                    seconds = getValue(currentValue, negativeFlag);
                    microsecondFlag = true;
                } else if (microsecondFlag && c == 'S') {
                    microseconds = getMicroseconds(currentValue, negativeFlag);
                }

                negativeFlag = false;
                currentValue = 0;
                multiplier = 1;
            }
        }

        return of(years, months, days, hours, minutes, seconds, microseconds);
    }

    /**
     * Parses the {@code value} representing an {@link Interval} in postgres
     * or postgres_verbose format.
     *
     * @param value the value in postgres or postgres_verbose format
     * @return the new {@link Interval} object
     */
    private static Interval parsePostgresIntervalValue(String value) {

        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        boolean postgresVerbose = false;
        boolean negativeFlag = false;
        boolean agoFlag = false;
        boolean hoursFlag = false;
        boolean minutesFlag = false;
        boolean microsecondFlag = false;
        int currentValue = 0;
        int multiplier = 1;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == ' ' || c == '+') {
                continue;
            } else if (c == '@') {
                postgresVerbose = true;
            } else if (c == '-') {
                negativeFlag = true;
            } else if (Character.isDigit(c)) {
                currentValue = (currentValue * multiplier) + (c - '0');
                multiplier = 10;
            } else {

                if (matchesPrefixAtPosition(value, "year", i)) {
                    years = getValue(currentValue, negativeFlag);
                    i = skipToNextSpaceOrEnd(value, i);
                } else if (matchesPrefixAtPosition(value, "mon", i)) {
                    months = getValue(currentValue, negativeFlag);
                    i = skipToNextSpaceOrEnd(value, i);
                } else if (matchesPrefixAtPosition(value, "day", i)) {
                    days = getValue(currentValue, negativeFlag);
                    i = skipToNextSpaceOrEnd(value, i);
                } else if (postgresVerbose) {

                    if (matchesPrefixAtPosition(value, "hour", i)) {
                        hours = getValue(currentValue, negativeFlag);
                    } else if (matchesPrefixAtPosition(value, "min", i)) {
                        minutes = getValue(currentValue, negativeFlag);
                    } else if (!microsecondFlag && (c == '.' || matchesPrefixAtPosition(value, "sec", i))) {
                        seconds = getValue(currentValue, negativeFlag);
                        microsecondFlag = true;
                    } else if (microsecondFlag && matchesPrefixAtPosition(value, "sec", i)) {
                        microseconds = getMicroseconds(currentValue, negativeFlag);
                    } else if (matchesPrefixAtPosition(value, "ago", i)) {
                        agoFlag = true;
                    }

                    if (c != '.') {
                        i = skipToNextSpaceOrEnd(value, i);
                    }

                } else {

                    if (c == '.') {
                        microsecondFlag = true;
                        seconds = getValue(currentValue, negativeFlag);
                    } else {
                        if (!hoursFlag) {
                            hoursFlag = true;
                            hours = getValue(currentValue, negativeFlag);
                        } else if (!minutesFlag) {
                            minutesFlag = true;
                            minutes = getValue(currentValue, negativeFlag);
                        }
                    }

                    currentValue = 0;
                    multiplier = 1;
                    // keep the negativeFlag value
                    continue;
                }

                negativeFlag = false;
                currentValue = 0;
                multiplier = 1;
            }
        }

        if (!postgresVerbose) {
            if (!minutesFlag) {
                minutes = getValue(currentValue, negativeFlag);
            } else if (!microsecondFlag) {
                seconds = getValue(currentValue, negativeFlag);
            } else {
                microseconds = getMicroseconds(currentValue, negativeFlag);
            }
        }

        if (agoFlag) {
            // Inverse the leading sign
            return of(-years, -months, -days, -hours, -minutes, -seconds, -microseconds);
        } else {
            return of(years, months, days, hours, minutes, seconds, microseconds);
        }
    }

    /**
     * Returns the value with the correct sign.
     *
     * @param value        the value
     * @param negativeFlag the flag that indicates whether the value is negative
     * @return the negative value of the value when the negativeFlag is true, the original value otherwise
     */
    private static int getValue(int value, boolean negativeFlag) {
        return negativeFlag ? -value : value;
    }

    /**
     * Returns the microsecond from a given value.
     *
     * @param value         the value
     * @param negativeValue the flag that indicates whether the value is negative
     * @return the microseconds
     */
    private static int getMicroseconds(int value, boolean negativeValue) {
        while ((value * 10) < 1_000_000) {
            value *= 10;
        }
        return negativeValue ? -value : value;
    }

    /**
     * Skips to the next space or to the end of the string.
     *
     * @param value the value
     * @param pos   the starting position
     * @return the index of the next space, or the end of the string when the space is not found
     */
    private static int skipToNextSpaceOrEnd(String value, int pos) {
        int i = pos;
        while (i < value.length() && ' ' != value.charAt(i)) {
            i++;
        }
        return i;
    }

    /**
     * Returns true if the {@code value} at position {@code pos} matches the
     * given {@code prefix}.
     *
     * @param value  the value
     * @param prefix the prefix to match
     * @param pos    the starting position to compare
     * @return true if the value at position pos matches the prefix, false otherwise
     */
    private static boolean matchesPrefixAtPosition(String value, String prefix, int pos) {
        int j;
        for (j = 0; j < prefix.length() && pos + j < value.length(); j++) {
            if (value.charAt(j + pos) != prefix.charAt(j))
                return false;
        }
        return j == prefix.length();
    }
}
