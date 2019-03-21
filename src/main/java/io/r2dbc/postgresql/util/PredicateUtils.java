/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.util;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Utilities for working with {@link Predicate}s.
 */
public final class PredicateUtils {

    private PredicateUtils() {
    }

    /**
     * Negates a {@link Predicate}.  Exists primarily to enable negation of method references that are {@link Predicate}s.
     *
     * @param t   the predicate to negate
     * @param <T> the type of element being tested
     * @return a negated predicate
     * @throws IllegalArgumentException if {@code t} is {@code null}
     * @see Predicate#negate()
     */
    public static <T> Predicate<T> not(Predicate<T> t) {
        Assert.requireNonNull(t, "t must not be null");

        return t.negate();
    }

    /**
     * Logical OR a collection of {@link Predicate}s.  Exists primarily to enable the logical OR of method references that are {@link Predicate}s.
     *
     * @param ts  the predicates to logical OR
     * @param <T> the type of element being tested
     * @return a local ORd collection of predicates
     * @throws IllegalArgumentException if {@code ts} is {@code null}
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Predicate<T> or(Predicate<T>... ts) {
        Assert.requireNonNull(ts, "ts must not be null");

        return Arrays.stream(ts).reduce(Predicate::or).orElseThrow(() -> new IllegalStateException("Unable to combine predicates together via logical OR"));
    }

}
