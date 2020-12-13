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

import org.assertj.core.api.AbstractAssert;

/**
 * Assertion utility for {@link Interval}.
 */
public class IntervalAssert extends AbstractAssert<IntervalAssert, Interval> {

    private IntervalAssert(Interval actual) {
        super(actual, IntervalAssert.class);
    }

    public static IntervalAssert assertThat(Interval actual) {
        return new IntervalAssert(actual);
    }

    public IntervalAssert hasYears(int expected) {
        isNotNull();

        if (this.actual.getYears() != expected) {
            failWithMessage("Expected interval's years to be <%s> but was <%s>", expected, this.actual.getYears());
        }

        return this;
    }

    public IntervalAssert hasMonths(int expected) {
        isNotNull();

        if (this.actual.getMonths() != expected) {
            failWithMessage("Expected interval's months to be <%s> but was <%s>", expected, this.actual.getMonths());
        }

        return this;
    }

    public IntervalAssert hasDays(int expected) {
        isNotNull();

        if (this.actual.getDays() != expected) {
            failWithMessage("Expected interval's days to be <%s> but was <%s>", expected, this.actual.getDays());
        }

        return this;
    }

    public IntervalAssert hasHours(int expected) {
        isNotNull();

        if (this.actual.getHours() != expected) {
            failWithMessage("Expected interval's hours to be <%s> but was <%s>", expected, this.actual.getHours());
        }

        return this;
    }

    public IntervalAssert hasMinutes(int expected) {
        isNotNull();

        if (this.actual.getMinutes() != expected) {
            failWithMessage("Expected interval's minutes to be <%s> but was <%s>", expected, this.actual.getMinutes());
        }

        return this;
    }

    public IntervalAssert hasSeconds(double expected) {
        isNotNull();

        if (this.actual.getSeconds() != expected) {
            failWithMessage("Expected interval's seconds to be <%s> but was <%s>", expected, this.actual.getSeconds());
        }

        return this;
    }

    public IntervalAssert isNegative() {
        isNotNull();

        if (!this.actual.isNegative()) {
            failWithMessage("Expected interval to be negative but was <%s>", this.actual.isNegative());
        }

        return this;
    }

    public IntervalAssert isNotNegative() {
        isNotNull();

        if (this.actual.isNegative()) {
            failWithMessage("Expected interval to be not negative but was <%s>", !this.actual.isNegative());
        }

        return this;
    }

    public IntervalAssert isZero() {
        isNotNull();

        if (!this.actual.isZero()) {
            failWithMessage("Expected interval to be zero but was <%s>", false);
        }

        return this;
    }

    public IntervalAssert isNotZero() {
        isNotNull();

        if (this.actual.isZero()) {
            failWithMessage("Expected interval to be not zero but was <%s>", false);
        }

        return this;
    }

}
