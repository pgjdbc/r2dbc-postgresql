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

package io.r2dbc.postgresql.util;

import reactor.util.annotation.Nullable;

import java.io.File;
import java.net.URL;

/**
 * Assertion library for the implementation.
 */
public final class Assert {

    private Assert() {
    }

    /**
     * Checks that a specified value is an array with a given dimension.
     *
     * @param type      the value to check
     * @param dimension the dimension to require
     * @param message   the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @param <T>       the type of the reference
     * @return {@code t} if dimension matches
     * @throws IllegalArgumentException if {@code t} is not of the required dimension
     */
    public static <T> Class<T> requireArrayDimension(Class<T> type, int dimension, String message) {
        requireNonNull(type, "t must not be null");
        requireNonNull(message, "message must not be null");

        Class<?> t = type;

        int d = 0;
        while (t.isArray()) {
            t = t.getComponentType();
            d++;
        }

        if (d != dimension) {
            throw new IllegalArgumentException(String.format(message, d, dimension));
        }

        return type;
    }

    /**
     * Checks that a specified object reference is not {@code null} and throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param t       the object reference to check for nullity
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @param <T>     the type of the reference
     * @return {@code t} if not {@code null}
     * @throws IllegalArgumentException if {@code t} is {code null}
     */
    public static <T> T requireNonNull(@Nullable T t, String message) {
        if (t == null) {
            throw new IllegalArgumentException(message);
        }

        return t;
    }

    /**
     * Checks that a specified {@link String} is not {@code null} and not {@link String#isEmpty() empty} and throws a customized {@link IllegalArgumentException} if it is.
     *
     * @param t       the  {@link String} reference to check for nullity and emptiness
     * @param message the detail message to be used in the event that an {@link IllegalArgumentException} is thrown
     * @return {@code t} if not {@code null} or {@link String#isEmpty() empty}
     * @throws IllegalArgumentException if {@code t} is {code null}
     */
    public static String requireNotEmpty(@Nullable String t, String message) {
        if (t == null || t.isEmpty()) {
            throw new IllegalArgumentException(message);
        }

        return t;
    }

    /**
     * Checks that the specified value is of a specific type.
     *
     * @param value   the value to check
     * @param type    the type to require
     * @param message the message to use in exception if type is not as required
     * @param <T>     the type being required
     * @return the value casted to the required type
     * @throws IllegalArgumentException if {@code value} is not of the required type
     * @throws IllegalArgumentException if {@code value}, {@code type}, or {@code message} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static <T> T requireType(Object value, Class<T> type, String message) {
        requireNonNull(value, "value must not be null");
        requireNonNull(type, "type must not be null");
        requireNonNull(message, "message must not be null");

        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(message);
        }

        return (T) value;
    }

    /**
     * Checks that the provided file exists or null.
     *
     * @param file    file to check
     * @param message the message to use in exception if type is not as required
     * @return existing file or null
     * @throws IllegalArgumentException if {@code value} is not of the required type
     * @throws IllegalArgumentException if {@code value}, {@code type}, or {@code message} is {@code null}
     */
    public static String requireFileExistsOrNull(@Nullable String file, String message) {
        if (file == null) {
            throw new IllegalArgumentException(message);
        }
        if (!new File(file).exists()) {
            throw new IllegalArgumentException(message);
        }
        return file;
    }

    /**
     * Assert a boolean expression, throwing an {@link IllegalArgumentException}
     * if the expression evaluates to {@code false}.
     *
     * @param expression a boolean expression
     * @param message    the exception message to use if the assertion fails
     * @throws IllegalArgumentException if {@code expression} is {@code false}
     */
    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks that the provided URL exists or null.
     *
     * @param url    url to check
     * @param message the message to use in exception if type is not as required
     * @return existing url
     * @throws IllegalArgumentException if {@code value} is not of the required type
     * @throws IllegalArgumentException if {@code value}, {@code type}, or {@code message} is {@code null}
     */
    public static URL requireUrlExistsOrNull(@Nullable URL url, String message) {
        if (url == null) {
            throw new IllegalArgumentException(message);
        }
        if (!new File(url.getFile()).exists()) {
            throw new IllegalArgumentException(message);
        }
        return url;
    }

}
