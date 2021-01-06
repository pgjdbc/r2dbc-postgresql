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

import io.r2dbc.postgresql.util.Assert;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.chrono.ChronoPeriod;
import java.time.chrono.Chronology;
import java.time.chrono.IsoChronology;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

/**
 * Value object that maps to the {@code interval} datatype in Postgres.
 * <p>
 * This class models a quantity or amount of time in terms of years, months, days, hours, minutes, seconds, and microseconds.
 * In addition to accessing its individual properties, its values can be retrieved as {@link #getPeriod() Period} and {@link Duration}.
 * <p>
 * A physical duration could be of infinite length.
 * For practicality, the duration is stored using integers without normalization.
 * {@link #normalized() Normalizing} applies separate normalization following {@link IsoChronology}.
 * Days/hours/minutes/seconds/microseconds and years/months are normalized independent from each other to avoid month rollover as the amount of days per month varies by per month and leap year. Day
 * normalization (e.g. more than 28/29/30/31 days) can only be applied in the context of an actual date hence it's not supported by this class.
 * Normalizing an interval of 14 months and 43 days results in an interval of 1 year, 2 months, and 43 days.
 * <p>
 * An {@code interval} can be created either from a {@code ISO-8601} or Postgres representation using {@link #parse(String)}. It can also be created from a {@link TemporalAmount} or as function
 * accepting {@link TemporalAmount temporal} {@code start} and {@code end} values through {@link #between(Temporal, Temporal)}.
 * <p>This class is immutable and thread-safe.
 *
 * @see Duration
 * @see Period
 * @see ChronoPeriod
 * @since 0.8.7
 */
public final class Interval implements ChronoPeriod, Serializable {

    /**
     * Constant for a duration of zero.
     */
    public static final Interval ZERO = new Interval(0, 0, 0, 0, 0, 0, 0);

    /**
     * The supported units.
     */
    private static final List<TemporalUnit> SUPPORTED_UNITS =
        Collections.unmodifiableList(Arrays.<TemporalUnit>asList(ChronoUnit.YEARS, ChronoUnit.MONTHS, ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.SECONDS, ChronoUnit.MICROS));

    /**
     * The number of seconds per day.
     */
    private static final long SECONDS_PER_DAY = 86400;

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
        Assert.requireNonNull(period, "period must not be null");

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
        Assert.requireNonNull(duration, "duration must not be null");

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
        Assert.requireNonNull(period, "period must not be null");
        Assert.requireNonNull(duration, "duration must not be null");

        if (Period.ZERO.equals(period) && Duration.ZERO.equals(duration)) {
            return Interval.ZERO;
        }

        return new Interval(period, duration);
    }

    /**
     * Create a new {@link Interval} given the {@code years}, {@code months},
     * {@code days}, {@code hours}, {@code minutes}, and {@code seconds}.
     *
     * @param years   interval years
     * @param months  interval months
     * @param days    interval days
     * @param hours   interval hours
     * @param minutes interval minutes
     * @param seconds interval seconds including microseconds as fraction
     * @return the new {@link Interval} object
     */
    public static Interval of(int years, int months, int days, int hours, int minutes, double seconds) {
        if ((years | months | days | hours | minutes) == 0 && seconds == 0) {
            return ZERO;
        }

        int secondsInMinute = (int) seconds % SECONDS_IN_MINUTE;
        int microseconds = Math.toIntExact(Math.round((seconds - secondsInMinute) * 1000 * 1000));

        return new Interval(years, months, days, hours, minutes, secondsInMinute, microseconds);
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
     * Create a new {@link Interval} consisting of the amount of time between two {@link Temporal}s.
     * <p>
     * The start is included, but the end is not.
     * The result of this method can be negative if the end is before the start.
     * <p>
     * The calculation examines the temporals and extracts {@link LocalDate} and {@link LocalTime}.
     * If the time is missing, it will be defaulted to midnight.
     * If one date is missing, it will be defaulted to the other date.
     * It then finds the amount of time between the two dates and between the two times.
     *
     * @param startInclusive the start, inclusive
     * @param endExclusive   the end, exclusive
     * @return the number of days between this date and the end date, not {@code null}
     * @throws IllegalArgumentException if {@code startInclusive} or {@code endExclusive} is {@code null}
     */
    public static Interval between(Temporal startInclusive, Temporal endExclusive) {

        Assert.requireNonNull(startInclusive, "startInclusive must not be null");
        Assert.requireNonNull(endExclusive, "endExclusive must not be null");

        LocalDate startDate = startInclusive.query(TemporalQueries.localDate());
        LocalDate endDate = endExclusive.query(TemporalQueries.localDate());

        Period period = Period.ZERO;
        if (startDate != null && endDate != null) {
            period = Period.between(startDate, endDate);
        }

        LocalTime startTime = startInclusive.query(TemporalQueries.localTime());
        LocalTime endTime = endExclusive.query(TemporalQueries.localTime());
        startTime = startTime != null ? startTime : LocalTime.MIDNIGHT;
        endTime = endTime != null ? endTime : LocalTime.MIDNIGHT;
        Duration duration = Duration.between(startTime, endTime);

        return Interval.of(period, duration);
    }

    /**
     * Create a new {@link Interval} from a {@link TemporalAmount temporal amount}.
     * <p>
     * This obtains an instance based on the specified amount.
     * A {@link TemporalAmount} represents an amount of time which this factory
     * extracts to a {@link Interval}.
     * <p>
     * The result is calculated by looping around each unit in the specified amount.
     * Any amount that is zero is ignore.
     * If a unit has an exact duration, it will be totalled using {@link Duration#plus(Duration)}.
     * If the unit is days or weeks, it will be totalled into the days part of the period.
     * If the unit is months or quarters, it will be totalled into the months part of the period.
     * If the unit is years, decades, centuries or millennia, it will be totalled into the years part of the period.
     *
     * @param amount the temporal amount to convert
     * @return the equivalent duration
     * @throws DateTimeException        if unable to convert to a {@code Duration}
     * @throws ArithmeticException      if numeric overflow occurs
     * @throws IllegalArgumentException if {@code amount} is {@code null}
     */
    public static Interval from(TemporalAmount amount) {

        Assert.requireNonNull(amount, "amount must not be null");

        if (amount instanceof Interval) {
            return (Interval) amount;
        }

        if (amount instanceof Period) {
            return Interval.of((Period) amount);
        }

        if (amount instanceof Duration) {
            return Interval.of((Duration) amount);
        }

        if (amount instanceof ChronoPeriod) {
            if (!IsoChronology.INSTANCE.equals(((ChronoPeriod) amount).getChronology())) {
                throw new DateTimeException("Period requires ISO chronology: " + amount);
            }
        }

        int years = 0;
        int months = 0;
        int days = 0;
        Duration duration = Duration.ZERO;
        for (TemporalUnit unit : amount.getUnits()) {
            long value = amount.get(unit);
            if (value != 0) {
                // ignore unless non-zero
                if (unit.isDurationEstimated()) {
                    if (unit == ChronoUnit.DAYS) {
                        days = Math.addExact(days, Math.toIntExact(value));
                    } else if (unit == ChronoUnit.WEEKS) {
                        days = Math.addExact(days, Math.toIntExact(Math.multiplyExact(value, 7)));
                    } else if (unit == ChronoUnit.MONTHS) {
                        months = Math.addExact(months, Math.toIntExact(value));
                    } else if (unit == IsoFields.QUARTER_YEARS) {
                        months = Math.addExact(months, Math.toIntExact(Math.multiplyExact(value, 3)));
                    } else if (unit == ChronoUnit.YEARS) {
                        years = Math.addExact(years, Math.toIntExact(value));
                    } else if (unit == ChronoUnit.DECADES) {
                        years = Math.addExact(years, Math.toIntExact(Math.multiplyExact(value, 10)));
                    } else if (unit == ChronoUnit.CENTURIES) {
                        years = Math.addExact(years, Math.toIntExact(Math.multiplyExact(value, 100)));
                    } else if (unit == ChronoUnit.MILLENNIA) {
                        years = Math.addExact(years, Math.toIntExact(Math.multiplyExact(value, 1000)));
                    } else {
                        throw new DateTimeException("Unknown unit: " + unit);
                    }
                } else {
                    // total of exact durations
                    duration = duration.plus(amount.get(unit), unit);
                }
            }
        }
        return Interval.of(Period.of(years, months, days), duration);
    }

    /**
     * Parse the {@code value} representing a {@link Interval}. This method
     * only supports values that Postgres returns.
     *
     * @param value a string that represents the interval value
     * @return the new {@link Interval} object
     * @throws IllegalArgumentException if {@code value} is {@code null} or contains an invalid format
     */
    public static Interval parse(String value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.startsWith("P") ?
            parseISO8601IntervalValue(value) :
            parsePostgresIntervalValue(value);
    }

    /**
     * Return the years represented by this interval.
     *
     * @return the years represented by this interval
     */
    public int getYears() {
        return this.years;
    }

    /**
     * Return the months represented by this interval.
     *
     * @return the months represented by this interval
     */
    public int getMonths() {
        return this.months;
    }

    /**
     * Return the days represented by this interval.
     *
     * @return the days represented by this interval
     */
    public int getDays() {
        return this.days;
    }

    /**
     * Return the hours represented by this interval.
     *
     * @return the hours represented by this interval
     */
    public int getHours() {
        return this.hours;
    }

    /**
     * Return the hours represented by this interval.
     *
     * @return the hours represented by this interval
     */
    public int getMinutes() {
        return this.minutes;
    }

    /**
     * Return the seconds represented by this interval.
     *
     * @return seconds represented by this interval
     */
    public double getSeconds() {
        return this.seconds + (double) this.microseconds / MICROSECONDS_IN_SECOND;
    }

    /**
     * Return the seconds represented by this interval.
     *
     * @return seconds represented by this interval
     */
    public int getSecondsInMinute() {
        return this.seconds;
    }

    /**
     * Return the microseconds represented by this interval.
     *
     * @return microseconds represented by this interval
     */
    public int getMicrosecondsInSecond() {
        return this.microseconds;
    }

    @Override
    public long get(TemporalUnit unit) {

        if (unit instanceof ChronoUnit) {
            switch ((ChronoUnit) unit) {
                case YEARS:
                    return this.years;
                case MONTHS:
                    return this.months;
                case DAYS:
                    return this.days;
                case HOURS:
                    return this.hours;
                case MINUTES:
                    return this.minutes;
                case SECONDS:
                    return this.seconds;
                case MICROS:
                    return this.microseconds;
                default:
                    break;
            }
        }
        throw new UnsupportedTemporalTypeException(String.format("Unsupported unit: %s", unit));
    }

    @Override
    public List<TemporalUnit> getUnits() {
        return SUPPORTED_UNITS;
    }

    @Override
    public Temporal addTo(Temporal temporal) {
        Assert.requireNonNull(temporal, "temporal must not be null");

        return temporal.plus(getPeriod()).plus(getDuration());
    }

    @Override
    public Temporal subtractFrom(Temporal temporal) {
        Assert.requireNonNull(temporal, "temporal must not be null");

        return temporal.minus(getPeriod()).minus(getDuration());
    }

    @Override
    public Interval plus(TemporalAmount amountToAdd) {

        Assert.requireNonNull(amountToAdd, "amountToAdd must not be null");

        Interval other = Interval.from(amountToAdd);
        return of(getPeriod().plus(other.getPeriod()), getDuration().plus(other.getDuration()));
    }

    @Override
    public Interval minus(TemporalAmount amountToSubtract) {

        Assert.requireNonNull(amountToSubtract, "amountToSubtract must not be null");

        Interval other = Interval.from(amountToSubtract);
        return of(getPeriod().minus(other.getPeriod()), getDuration().minus(other.getDuration()));
    }

    @Override
    public Chronology getChronology() {
        return IsoChronology.INSTANCE;
    }

    @Override
    public Interval multipliedBy(int scalar) {
        return Interval.of(Math.multiplyExact(this.years, scalar), Math.multiplyExact(this.months, scalar), Math.multiplyExact(this.days, scalar), Math.multiplyExact(this.hours, scalar),
            Math.multiplyExact(this.minutes, scalar), Math.multiplyExact(this.seconds, scalar), Math.multiplyExact(this.microseconds, scalar));
    }

    @Override
    public Interval normalized() {

        long remainderMicroseconds = this.microseconds + TimeUnit.SECONDS.toMicros(this.seconds)
            + TimeUnit.MINUTES.toMicros(this.minutes) + TimeUnit.HOURS.toMicros(this.hours) + TimeUnit.DAYS.toMicros(this.days);

        int days = Math.toIntExact(remainderMicroseconds / (MICROSECONDS_IN_SECOND * SECONDS_PER_DAY));
        remainderMicroseconds -= days * (MICROSECONDS_IN_SECOND * SECONDS_PER_DAY);

        int hours = Math.toIntExact(remainderMicroseconds / (MICROSECONDS_IN_SECOND * 3600L));
        remainderMicroseconds -= hours * (MICROSECONDS_IN_SECOND * 3600L);

        int minutes = Math.toIntExact(remainderMicroseconds / (MICROSECONDS_IN_SECOND * 60L));
        remainderMicroseconds -= minutes * (MICROSECONDS_IN_SECOND * 60L);

        int seconds = Math.toIntExact(remainderMicroseconds / MICROSECONDS_IN_SECOND);
        remainderMicroseconds -= seconds * (long) MICROSECONDS_IN_SECOND;

        int microseconds = Math.toIntExact(remainderMicroseconds % MICROSECONDS_IN_SECOND);

        int totalMonths = this.months + (this.years * 12);
        int splitYears = totalMonths / 12;
        int splitMonths = totalMonths % 12;

        return Interval.of(splitYears, splitMonths, days, hours, minutes, seconds, microseconds);
    }

    /**
     * Return a copy instance of {@link Interval} applying the current interval and adjusting years given {@code years}.
     *
     * @param years the new years value to apply
     * @return a new instance of {@link Interval} with {@code years} applied
     */
    public Interval withYears(int years) {
        return new Interval(years, this.months, this.days, this.hours, this.minutes, this.seconds, this.microseconds);
    }

    /**
     * Return a copy a new instance of {@link Interval} applying the current interval and adjusting years given {@code months}.
     *
     * @param months the new months value to apply
     * @return a new instance of {@link Interval} with {@code months} applied
     */
    public Interval withMonths(int months) {
        return new Interval(this.years, months, this.days, this.hours, this.minutes, this.seconds, this.microseconds);
    }

    /**
     * Return a new instance of {@link Interval} applying the current interval and adjusting years given {@code days}.
     *
     * @param days the new days value to apply
     * @return a new instance of {@link Interval} with {@code days} applied
     */
    public Interval withDays(int days) {
        return new Interval(this.years, this.months, days, this.hours, this.minutes, this.seconds, this.microseconds);
    }

    /**
     * Return a new instance of {@link Interval} applying the current interval and adjusting years given {@code hours}.
     *
     * @param hours the new hours value to apply
     * @return a new instance of {@link Interval} with {@code hours} applied
     */
    public Interval withHours(int hours) {
        return new Interval(this.years, this.months, this.days, hours, this.minutes, this.seconds, this.microseconds);
    }

    /**
     * Return a new instance of {@link Interval} applying the current interval and adjusting years given {@code minutes}.
     *
     * @param minutes the new minutes value to apply
     * @return a new instance of {@link Interval} with {@code minutes} applied
     */
    public Interval withMinutes(int minutes) {
        return new Interval(this.years, this.months, this.days, this.hours, minutes, this.seconds, this.microseconds);
    }

    /**
     * Return a new instance of {@link Interval} applying the current interval and adjusting years given {@code seconds}.
     *
     * @param seconds the new seconds value to apply
     * @return a new instance of {@link Interval} with {@code seconds} applied
     */
    public Interval withSeconds(int seconds) {
        return new Interval(this.years, this.months, this.days, this.hours, this.minutes, seconds, this.microseconds);
    }

    /**
     * Return a new instance of {@link Interval} applying the current interval and adjusting years given {@code microseconds}.
     *
     * @param microseconds the new microseconds value to apply
     * @return a new instance of {@link Interval} with {@code seconds} applied
     */
    public Interval withMicroseconds(int microseconds) {
        return new Interval(this.years, this.months, this.days, this.hours, this.minutes, this.seconds, microseconds);
    }

    /**
     * Return the years, months, and days as {@link Period}.
     *
     * @return the years, months, and days as {@link Period}.
     */
    public Period getPeriod() {
        return Period.of(this.years, this.months, this.days);
    }

    /**
     * Return the hours, minutes, seconds and microseconds as {@link Duration}.
     *
     * @return the hours, minutes, seconds and microseconds as {@link Duration}.
     */
    public Duration getDuration() {
        return Duration.ofHours(this.hours).plusMinutes(this.minutes).plusSeconds(this.seconds).plus(this.microseconds, ChronoUnit.MICROS);
    }

    /**
     * Return a string representing the interval value.
     *
     * @return a string representing the interval value
     */
    public String getValue() {

        DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        df.applyPattern("0.0#####");

        return String.format(
            Locale.ROOT,
            "%d years %d mons %d days %d hours %d mins %s secs",
            this.years,
            this.months,
            this.days,
            this.hours,
            this.minutes,
            df.format(getSeconds())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Interval)) {
            return false;
        }
        Interval interval = (Interval) o;
        return this.years == interval.years && this.months == interval.months && this.days == interval.days && this.hours == interval.hours && this.minutes == interval.minutes && this.seconds == interval.seconds && this.microseconds == interval.microseconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.years, this.months, this.days, this.hours, this.minutes, this.seconds, this.microseconds);
    }

    @Override
    public String toString() {
        return getValue();
    }

    /**
     * Parse the ISO8601 {@code value} representing an {@link Interval}.
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
     * Parse the {@code value} representing an {@link Interval} in postgres
     * or postgres_verbose format.
     *
     * @param value the value in postgres or postgres_verbose format
     * @return the new {@link Interval} object
     */
    private static Interval parsePostgresIntervalValue(String value) {

        boolean isVerbose = value.startsWith("@");

        // Just a simple '0'
        if (isVerbose && value.length() == 3 && value.charAt(2) == '0') {
            return Interval.of(Duration.ZERO);
        }

        int years = 0;
        int months = 0;
        int days = 0;
        int hours = 0;
        int minutes = 0;
        double seconds = 0;
        boolean ago = false;

        try {

            String valueToken;
            String changedValue = value.replace('+', ' ').replace('@', ' ');

            StringTokenizer st = new StringTokenizer(changedValue);
            while (st.hasMoreTokens()) {

                String token = st.nextToken();

                if (token.equals("ago")) {
                    ago = true;
                    break;
                }

                int endHours = token.indexOf(':');
                if (endHours == -1) {
                    valueToken = token;
                } else {

                    // This handles hours, minutes, seconds and microseconds for
                    // ISO intervals
                    int offset = (token.charAt(0) == '-') ? 1 : 0;

                    hours = Integer.parseInt(token.substring(offset, endHours));
                    minutes = Integer.parseInt(token.substring(endHours + 1, endHours + 3));

                    // Pre 7.4 servers do not put second information into the results
                    // unless it is non-zero.
                    int endMinutes = token.indexOf(':', endHours + 1);
                    if (endMinutes != -1) {
                        NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ROOT);
                        seconds = numberFormat.parse(token.substring(endMinutes + 1)).doubleValue();
                    }

                    if (offset == 1) {
                        hours = -hours;
                        minutes = -minutes;
                        seconds = -seconds;
                    }

                    break;
                }

                if (!st.hasMoreTokens()) {
                    throw new IllegalArgumentException("Invalid interval: " + value);
                }

                token = st.nextToken();

                // This handles years, months, days for both, ISO and
                // Non-ISO intervals. Hours, minutes, seconds and microseconds
                // are handled for Non-ISO intervals here.

                if (token.startsWith("year") || token.startsWith("yr")) {
                    years = Integer.parseInt(valueToken);
                } else if (token.startsWith("mon")) {
                    months = Integer.parseInt(valueToken);
                } else if (token.startsWith("day")) {
                    days = Integer.parseInt(valueToken);
                } else if (token.startsWith("hour") || token.startsWith("hr")) {
                    hours = Integer.parseInt(valueToken);
                } else if (token.startsWith("min")) {
                    minutes = Integer.parseInt(valueToken);
                } else if (token.startsWith("sec")) {
                    seconds = Double.parseDouble(valueToken);
                }

            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid interval: " + value, e);
        }

        if (isVerbose && ago) {
            // Inverse the leading sign
            years = -years;
            months = -months;
            days = -days;
            hours = -hours;
            minutes = -minutes;
            seconds = -seconds;
        }

        return Interval.of(years, months, days, hours, minutes, seconds);
    }

    /**
     * Return the value with the correct sign.
     *
     * @param value        the value
     * @param negativeFlag the flag that indicates whether the value is negative
     * @return the negative value of the value when the negativeFlag is true, the original value otherwise
     */
    private static int getValue(int value, boolean negativeFlag) {
        return negativeFlag ? -value : value;
    }

    /**
     * Return the microsecond from a given value.
     *
     * @param value         the value
     * @param negativeValue the flag that indicates whether the value is negative
     * @return the microseconds
     */
    private static int getMicroseconds(int value, boolean negativeValue) {
        while (value != 0 && (value * 10) < 1_000_000) {
            value *= 10;
        }
        return negativeValue ? -value : value;
    }

}
