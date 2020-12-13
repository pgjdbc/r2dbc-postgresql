/*
 * Copyright 2020 the original author or authors.
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.chrono.Chronology;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.r2dbc.postgresql.codec.IntervalAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link Interval}.
 */
final class IntervalUnitTests {

    static Stream<Object[]> parseValues() {
        return Stream.of(

            // intervalstyle=postgres
            new Object[]{"3 days 04:05:06", Interval.of(0, 0, 3, 4, 5, 6, 0)},
            new Object[]{"1 year 2 mon 3 day 04:05:06", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"185 years 11 mons 23 days 14:56:46.5456", Interval.of(185, 11, 23, 14, 56, 46, 545600)},
            new Object[]{"8 years 2 mons 3 days 04:05:06.545678", Interval.of(8, 2, 3, 4, 5, 6, 545678)},
            new Object[]{"-83 years -11 mons -3 days 04:05:06.545679", Interval.of(-83, -11, -3, 4, 5, 6, 545679)},
            new Object[]{"-9 years -7 mons -2 days +04:58:20", Interval.of(-9, -7, -2, 4, 58, 20, 0)},
            new Object[]{"5 days", Interval.of(Period.ofDays(5))},
            new Object[]{"1 year 2 mons", Interval.of(Period.of(1, 2, 0))},
            new Object[]{"1 year 2 mons 3 days 04:05:06", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"-100 years", Interval.of(Period.ofYears(-100))},
            new Object[]{"140:00:00", Interval.of(0, 0, 0, 140, 0, 0, 0)},
            new Object[]{"8 years 2 mons 3 days 04:05:06", Interval.of(8, 2, 3, 4, 5, 6, 0)},
            new Object[]{"8 years 2 mons 3 days 04:05:06.54", Interval.of(8, 2, 3, 4, 5, 6, 540000)},
            new Object[]{"8 years 0 mons 3 days 04:05:06.545", Interval.of(8, 0, 3, 4, 5, 6, 545000)},
            new Object[]{"8 years 2 mons 3 days 04:05:06.54567", Interval.of(8, 2, 3, 4, 5, 6, 545670)},
            new Object[]{"8 years 2 mons 3 days 04:05", Interval.of(8, 2, 3, 4, 5, 0, 0)},
            new Object[]{"00:00:00", Interval.ZERO},
            new Object[]{"-1 years -2 mons +3 days -04:05:06", Interval.of(-1, -2, 3, -4, -5, -6, 0)},

            // intervalstyle=postgres_verbose
            new Object[]{"@ 3 days 4 hours 5 mins 6 secs", Interval.of(0, 0, 3, 4, 5, 6, 0)},
            new Object[]{"@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"@ 185 yr 11 mon 23 day 14 hr 56 min 46.5456 sec", Interval.of(185, 11, 23, 14, 56, 46, 545600)},
            new Object[]{"@ +8 years 2 mons 3 days 4 hours 5 mins 6.545678 secs", Interval.of(8, 2, 3, 4, 5, 6, 545678)},
            new Object[]{"@ -83 years -11 mons -3 days 4 hours 5 mins 6.545679 secs", Interval.of(-83, -11, -3, 4, 5, 6, 545679)},
            new Object[]{"@ 9 years 7 mons 2 days -4 hours -58 mins -20 secs ago", Interval.of(-9, -7, -2, 4, 58, 20, 0)},
            new Object[]{"@ 5 days", Interval.of(Period.ofDays(5))},
            new Object[]{"@ 1 year 2 mons", Interval.of(Period.of(1, 2, 0))},
            new Object[]{"@ 1 year 2 mons 3 days 4 hours 5 mins 6 secs", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"@ 100 years ago", Interval.of(Period.ofYears(-100))},
            new Object[]{"@ 40 hours", Interval.of(0, 0, 0, 40, 0, 0, 0)},
            new Object[]{"@ 8 years 2 mons 3 days 4 hours 5 mins 6 secs", Interval.of(8, 2, 3, 4, 5, 6, 0)},
            new Object[]{"@ 8 years 2 mons 3 days 4 hours 5 mins 6.54 secs", Interval.of(8, 2, 3, 4, 5, 6, 540000)},
            new Object[]{"@ 8 years 2 mons 3 days 4 hours 5 mins 6.545 secs", Interval.of(8, 2, 3, 4, 5, 6, 545000)},
            new Object[]{"@ 8 years 2 mons 3 days 4 hours 5 mins 6.54567 secs", Interval.of(8, 2, 3, 4, 5, 6, 545670)},
            new Object[]{"@ 0", Interval.ZERO},
            new Object[]{"@ 1 year 2 mons -3 days 4 hours 5 mins 6 secs ago", Interval.of(-1, -2, 3, -4, -5, -6, 0)},

            // intervalstyle=iso_8601
            new Object[]{"P3DT4H5M6S", Interval.of(0, 0, 3, 4, 5, 6, 0)},
            new Object[]{"P1Y2M3DT4H5M6S", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"P185Y11M23DT14H56M46.5456S", Interval.of(185, 11, 23, 14, 56, 46, 545600)},
            new Object[]{"P8Y2M3DT4H5M6.545678S", Interval.of(8, 2, 3, 4, 5, 6, 545678)},
            new Object[]{"P-83Y-11M-3DT4H5M6.545679S", Interval.of(-83, -11, -3, 4, 5, 6, 545679)},
            new Object[]{"P-9Y-7M-2DT4H58M20S", Interval.of(-9, -7, -2, 4, 58, 20, 0)},
            new Object[]{"P5D", Interval.of(Period.ofDays(5))},
            new Object[]{"P1Y2M", Interval.of(Period.of(1, 2, 0))},
            new Object[]{"P1Y2M3DT4H5M6S", Interval.of(1, 2, 3, 4, 5, 6, 0)},
            new Object[]{"P-100Y", Interval.of(Period.ofYears(-100))},
            new Object[]{"PT140H", Interval.of(0, 0, 0, 140, 0, 0, 0)},
            new Object[]{"P8Y2M3DT4H5M6S", Interval.of(8, 2, 3, 4, 5, 6, 0)},
            new Object[]{"P8Y2M3DT4H5M6.54S", Interval.of(8, 2, 3, 4, 5, 6, 540000)},
            new Object[]{"P8Y2M3DT4H5M6.545S", Interval.of(8, 2, 3, 4, 5, 6, 545000)},
            new Object[]{"P8Y2M3DT4H5M6.54567S", Interval.of(8, 2, 3, 4, 5, 6, 545670)},
            new Object[]{"PT0S", Interval.ZERO},
            new Object[]{"P-1Y-2M3DT-4H-5M-6S", Interval.of(-1, -2, 3, -4, -5, -6, 0)}
        );
    }

    static Stream<Object[]> intervalValues() {
        return Stream.of(
            new Object[]{Interval.ZERO, "0 years 0 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofYears(178)), "178 years 0 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofMonths(11)), "0 years 11 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofWeeks(4)), "0 years 0 mons 28 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofDays(25)), "0 years 0 mons 25 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(58, 4, 0)), "58 years 4 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(3413, 0, 9)), "3413 years 0 mons 9 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(0, 2, 3)), "0 years 2 mons 3 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(99, 9, 9)), "99 years 9 mons 9 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofYears(-941592)), "-941592 years 0 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofMonths(-10)), "0 years -10 mons 0 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.ofDays(-17)), "0 years 0 mons -17 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(-523, 5, -21)), "-523 years 5 mons -21 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Period.of(-89, 0, 5)), "-89 years 0 mons 5 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Duration.ofDays(-98)), "0 years 0 mons -98 days 0 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Duration.ofHours(6)), "0 years 0 mons 0 days 6 hours 0 mins 0.0 secs"},
            new Object[]{Interval.of(Duration.ofMinutes(35)), "0 years 0 mons 0 days 0 hours 35 mins 0.0 secs"},
            new Object[]{Interval.of(Duration.ofSeconds(15)), "0 years 0 mons 0 days 0 hours 0 mins 15.0 secs"},
            new Object[]{Interval.of(Duration.ofMillis(753)), "0 years 0 mons 0 days 0 hours 0 mins 0.753 secs"},
            new Object[]{Interval.of(Duration.ofNanos(1000)), "0 years 0 mons 0 days 0 hours 0 mins 0.000001 secs"},
            new Object[]{Interval.of(Duration.ofSeconds(987654321)), "0 years 0 mons 11431 days 4 hours 25 mins 21.0 secs"},
            new Object[]{Interval.of(Duration.ofSeconds(443121)), "0 years 0 mons 5 days 3 hours 5 mins 21.0 secs"},
            new Object[]{Interval.of(Period.of(45, 6, 14), Duration.ofHours(3).plusMinutes(5).plusSeconds(21)), "45 years 6 mons 14 days 3 hours 5 mins 21.0 secs"},
            new Object[]{Interval.of(Period.of(-8124, -3, 1), Duration.ofHours(3).plusMinutes(5).plusSeconds(21)), "-8124 years -3 mons 1 days 3 hours 5 mins 21.0 secs"},
            new Object[]{Interval.of(Period.of(-8124, -3, 1), Duration.ofHours(-3).minusMinutes(5).minusSeconds(21)), "-8124 years -3 mons 1 days -3 hours -5 mins -21.0 secs"},
            new Object[]{Interval.of(2020, 11, 13, 12, 49, 39, 541039), "2020 years 11 mons 13 days 12 hours 49 mins 39.541039 secs"}
        );
    }

    @MethodSource("parseValues")
    @ParameterizedTest
    void intervalParse(String value, Interval expected) {
        assertThat(Interval.parse(value)).isEqualTo(expected);
    }

    @MethodSource("intervalValues")
    @ParameterizedTest
    void intervalGetValue(Interval value, String expected) {
        Assertions.assertThat(value.getValue()).isEqualTo(expected);
        Assertions.assertThat(Interval.parse(expected)).isEqualTo(value);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void intervalOfNoPeriod() {
        assertThatIllegalArgumentException().isThrownBy(() -> Interval.of(null, Duration.ZERO))
            .withMessage("period must not be null");
    }

    @Test
    void intervalOfPeriod() {

        assertThat(Interval.of(Period.ZERO)).isEqualTo(Interval.ZERO).isZero();

        // 3 years
        assertThat(Interval.of(Period.ofYears(3)))
            .hasYears(3)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotNegative()
            .isNotZero();

        // 2 months
        assertThat(Interval.of(Period.ofMonths(2)))
            .hasYears(0)
            .hasMonths(2)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotNegative()
            .isNotZero();

        // 37 months
        assertThat(Interval.of(Period.ofMonths(37)))
            .hasYears(3)
            .hasMonths(1)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // 74 days
        assertThat(Interval.of(Period.ofDays(74)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(74)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotNegative()
            .isNotZero();

        // 5 years 6 months 7 days
        assertThat(Interval.of(Period.of(5, 6, 7)))
            .hasYears(5)
            .hasMonths(6)
            .hasDays(7)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotNegative()
            .isNotZero();

        // 2 years 27 months 480 days
        assertThat(Interval.of(Period.of(2, 27, 480)))
            .hasYears(4)
            .hasMonths(3)
            .hasDays(480)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotNegative()
            .isNotZero();
    }

    @Test
    void intervalOfNegativePeriod() {

        // -10 years
        assertThat(Interval.of(Period.ofYears(-10)))
            .hasYears(-10)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();

        // -7 months
        assertThat(Interval.of(Period.ofMonths(-7)))
            .hasYears(0)
            .hasMonths(-7)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();

        // -56 months
        assertThat(Interval.of(Period.ofMonths(-56)))
            .hasYears(-4)
            .hasMonths(-8)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();

        // -3 days
        assertThat(Interval.of(Period.ofDays(-3)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(-3)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();

        // -2 years -3 months -4 days
        assertThat(Interval.of(Period.of(-2, -3, -4)))
            .hasYears(-2)
            .hasMonths(-3)
            .hasDays(-4)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();

        // -2 years -27 months -480 days
        assertThat(Interval.of(Period.of(-2, -27, -480)))
            .hasYears(-4)
            .hasMonths(-3)
            .hasDays(-480)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // -10 years -4 months -3 days
        assertThat(Interval.of(Period.of(-10, 4, 3)))
            .hasYears(-10)
            .hasMonths(4)
            .hasDays(3)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNegative();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void intervalOfNoDuration() {
        assertThatIllegalArgumentException().isThrownBy(() -> Interval.of(Period.ZERO, null))
            .withMessage("duration must not be null");
    }

    @Test
    void intervalOfDuration() {

        assertThat(Interval.of(Duration.ofSeconds(0))).isEqualTo(Interval.ZERO).isZero();

        // 530 days
        assertThat(Interval.of(Duration.ofDays(530)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(530)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0)
            .isNotZero();

        // 6 hours
        assertThat(Interval.of(Duration.ofHours(6)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(6)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // 5 minutes
        assertThat(Interval.of(Duration.ofMinutes(5)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(5)
            .hasSeconds(0.0);

        // 28 secsonds
        assertThat(Interval.of(Duration.ofSeconds(28)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(28.0);

        // 1 secsond 450 milliseconds
        assertThat(Interval.of(Duration.ofMillis(1450)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(1.45);

        // 927428 microseconds
        assertThat(Interval.of(Duration.of(927428, ChronoUnit.MICROS)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.927428);

        // 725 microseconds
        assertThat(Interval.of(Duration.of(725, ChronoUnit.MICROS)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.000725);

        // 1000 nanoseconds -- lower boundary
        assertThat(Interval.of(Duration.ofNanos(1000)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.000001);

        // 105 nanoseconds -- nanosecond precision is not supported
        assertThat(Interval.of(Duration.ofNanos(105)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // 530 days, 2 hours, 12 minutes, 25 secsonds
        assertThat(Interval.of(Duration.ofSeconds(45799945)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(530)
            .hasHours(2)
            .hasMinutes(12)
            .hasSeconds(25);

        // 14 days, 6 hours, 37 minutes, 59 secsonds
        assertThat(Interval.of(Duration.ofSeconds(1233479)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(14)
            .hasHours(6)
            .hasMinutes(37)
            .hasSeconds(59);

        // 23 days, 23 hours, 59 minutes, 3.35 secsonds
        assertThat(Interval.of(Duration.ofSeconds(2073543, 350000000)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(23)
            .hasHours(23)
            .hasMinutes(59)
            .hasSeconds(3.35);

        // Duration between
        assertThat(Interval.of(Duration
            .between(Instant.parse("2020-11-03T10:15:30.00Z"), Instant.parse("2020-11-07T10:16:30.00Z"))))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(4)
            .hasHours(0)
            .hasMinutes(1)
            .hasSeconds(0);
    }

    @Test
    void intervalOfNegativeDuration() {
        // -530 days
        assertThat(Interval.of(Duration.ofDays(-530)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(-530)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // -6 hours
        assertThat(Interval.of(Duration.ofHours(-6)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(-6)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // -5 minutes
        assertThat(Interval.of(Duration.ofMinutes(-5)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(-5)
            .hasSeconds(0.0);

        // -28 secsonds
        assertThat(Interval.of(Duration.ofSeconds(-28)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(-28.0);

        // -1 secsond -450 milliseconds
        assertThat(Interval.of(Duration.ofMillis(-1450)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(-1.45);

        // 927428 microseconds
        assertThat(Interval.of(Duration.of(-927428, ChronoUnit.MICROS)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(-0.927428);

        // -105 nanoseconds -- nanosecond precision is not supported
        assertThat(Interval.of(Duration.ofSeconds(-1, 1000000105)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(0)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // -530 days, -2 hours, -12 minutes, -25 secsonds
        assertThat(Interval.of(Duration.ofSeconds(-45799945)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(-530)
            .hasHours(-2)
            .hasMinutes(-12)
            .hasSeconds(-25);

        // -14 days, -6 hours, -37 minutes, -59 secsonds
        assertThat(Interval.of(Duration.ofSeconds(-1233479)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(-14)
            .hasHours(-6)
            .hasMinutes(-37)
            .hasSeconds(-59);

        // -23 days, -23 hours, -59 minutes, -3.35 secsonds
        assertThat(Interval.of(Duration.ofSeconds(-2073543, -350000000)))
            .hasYears(0)
            .hasMonths(0)
            .hasDays(-23)
            .hasHours(-23)
            .hasMinutes(-59)
            .hasSeconds(-3.35);
    }

    @Test
    void intervalOfPeriodAndDuration() {

        assertThat(Interval.of(Period.ZERO, Duration.ZERO)).isEqualTo(Interval.ZERO);

        // 3 years 6 hours
        assertThat(Interval.of(Period.ofYears(3), Duration.ofHours(6)))
            .hasYears(3)
            .hasMonths(0)
            .hasDays(0)
            .hasHours(6)
            .hasMinutes(0)
            .hasSeconds(0.0);

        // 2 years 27 months 480 days + 23 days, 23 hours, 59 minutes, 3.35 secsonds
        assertThat(Interval.of(Period.of(2, 27, 480), Duration.ofSeconds(2073543, 350000000)))
            .hasYears(4)
            .hasMonths(3)
            .hasDays(503)
            .hasHours(23)
            .hasMinutes(59)
            .hasSeconds(3.35);

        // 1 year 37 months 52 days + 14 days, 6 hours, 37 minutes, 59 secsonds
        assertThat(Interval.of(Period.of(1, 37, 52), Duration.ofSeconds(1233479)))
            .hasYears(4)
            .hasMonths(1)
            .hasDays(66)
            .hasHours(6)
            .hasMinutes(37)
            .hasSeconds(59);
    }

    @Test
    void between() {

        LocalDateTime start = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 0);
        LocalDateTime end = LocalDateTime.of(2001, 2, 3, 4, 5, 6, (int) TimeUnit.MICROSECONDS.toNanos(123));

        assertThat(Interval.between(start, end))
            .hasYears(1)
            .hasMonths(1)
            .hasDays(2)
            .hasHours(4)
            .hasMinutes(5)
            .hasSeconds(6.000123);
    }

    @Test
    void from() {

        assertThat(Interval.from(Chronology.ofLocale(Locale.US).period(1, 2, 3)))
            .hasYears(1)
            .hasMonths(2)
            .hasDays(3);

        assertThat(Interval.from(Period.of(1, 2, 3)))
            .hasYears(1)
            .hasMonths(2)
            .hasDays(3);

        assertThat(Interval.from(Duration.ofHours(1).plusMinutes(2).plusSeconds(3)))
            .hasHours(1)
            .hasMinutes(2)
            .hasSeconds(3);
    }

    @Test
    void withAdjustments() {

        Interval interval = Interval.parse("45 years 6 mons 14 days 3 hours 5 mins 21.0 secs");

        assertThat(interval).hasYears(45).hasMonths(6).hasDays(14).hasHours(3).hasMinutes(5).hasSeconds(21);

        assertThat(interval.withYears(123)).hasYears(123).hasMonths(6).hasDays(14).hasHours(3).hasMinutes(5).hasSeconds(21);
        assertThat(interval.withMonths(123)).hasYears(45).hasMonths(123).hasDays(14).hasHours(3).hasMinutes(5).hasSeconds(21);
        assertThat(interval.withDays(123)).hasYears(45).hasMonths(6).hasDays(123).hasHours(3).hasMinutes(5).hasSeconds(21);
        assertThat(interval.withHours(123)).hasYears(45).hasMonths(6).hasDays(14).hasHours(123).hasMinutes(5).hasSeconds(21);
        assertThat(interval.withMinutes(123)).hasYears(45).hasMonths(6).hasDays(14).hasHours(3).hasMinutes(123).hasSeconds(21);
        assertThat(interval.withSeconds(123)).hasYears(45).hasMonths(6).hasDays(14).hasHours(3).hasMinutes(5).hasSeconds(123);
        assertThat(interval.withMicroseconds(123)).hasYears(45).hasMonths(6).hasDays(14).hasHours(3).hasMinutes(5).hasSeconds(21.000123);
    }

    @Test
    void get() {

        Interval interval = Interval.of(1, 2, 3, 4, 5, 6, 7);

        Assertions.assertThat(interval.getSecondsInMinute()).isEqualTo(6);
        Assertions.assertThat(interval.getMicrosecondsInSecond()).isEqualTo(7);

        Assertions.assertThat(interval.get(ChronoUnit.YEARS)).isEqualTo(1);
        Assertions.assertThat(interval.get(ChronoUnit.MONTHS)).isEqualTo(2);
        Assertions.assertThat(interval.get(ChronoUnit.DAYS)).isEqualTo(3);
        Assertions.assertThat(interval.get(ChronoUnit.HOURS)).isEqualTo(4);
        Assertions.assertThat(interval.get(ChronoUnit.MINUTES)).isEqualTo(5);
        Assertions.assertThat(interval.get(ChronoUnit.SECONDS)).isEqualTo(6);
        Assertions.assertThat(interval.get(ChronoUnit.MICROS)).isEqualTo(7);
    }

    @Test
    void addTo() {

        Interval interval = Interval.of(Period.of(1, 0, 0));
        Temporal temporal = interval.addTo(LocalDate.of(1, 1, 1));

        Assertions.assertThat(((LocalDate) temporal).getYear()).isEqualTo(2);
    }

    @Test
    void subtractFrom() {

        Interval interval = Interval.of(Period.of(1, 0, 0));
        Temporal temporal = interval.subtractFrom(LocalDate.of(1, 1, 1));

        Assertions.assertThat(((LocalDate) temporal).getYear()).isEqualTo(0);
    }

    @Test
    void normalize() {

        assertThat(Interval.parse("28 days 26 hours 61 mins 71.123456 secs").normalized()).hasDays(29).hasHours(3).hasMinutes(2).hasSeconds(11.123456);

        // 28 - 24 -> 27, 27-2hr = 26d 22hr
        // 22 hr -60m = 21hr -1m = 20hr 59m
        // 59m - 1m = 58m - 11.123456sec = 57m 48.876544sec
        assertThat(Interval.parse("28 days -26 hours -61 mins -71.123456 secs").normalized()).hasDays(26).hasHours(20).hasMinutes(57).hasSeconds(48.876544);

        assertThat(Interval.parse("1 yr 14 mons").normalized()).hasYears(2).hasMonths(2);
        assertThat(Interval.parse("-1 yr 14 mons").normalized()).hasYears(0).hasMonths(2);
    }

    @Test
    void intervalParseOfNoValue() {
        String value = null;
        assertThatIllegalArgumentException().isThrownBy(() -> Interval.parse(value))
            .withMessage("value must not be null");
    }

}
