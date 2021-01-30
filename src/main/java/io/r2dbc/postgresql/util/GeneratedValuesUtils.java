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

package io.r2dbc.postgresql.util;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

/**
 * Utilities for working with generated values.
 */
public final class GeneratedValuesUtils {

    private static final Pattern DELETE = Pattern.compile(".*DELETE.*", CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern INSERT = Pattern.compile(".*INSERT.*", CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern RETURNING = Pattern.compile(".*\\bRETURNING\\b.*", CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern UPDATE = Pattern.compile(".*UPDATE.*", CASE_INSENSITIVE | Pattern.DOTALL);

    private GeneratedValuesUtils() {
    }

    /**
     * Augments an SQL statement with a {@code RETURNING} statement and column names.  If the collection is empty, uses {@code *} for column names.
     *
     * @param sql              the SQL to augment
     * @param generatedColumns the names of the columns to augment with
     * @return an augmented sql statement returning the specified columns or a wildcard
     * @throws IllegalArgumentException if {@code sql} or {@code generatedColumns} is {@code null}
     */
    public static String augment(String sql, String[] generatedColumns) {
        Assert.requireNonNull(sql, "sql must not be null");
        Assert.requireNonNull(generatedColumns, "generatedColumns must not be null");

        return String.format("%s RETURNING %s", sql, generatedColumns.length == 0 ? "*" : String.join(", ", generatedColumns));
    }

    /**
     * Returns {@code true} if the sql statement already has a {@code RETURNING} clause, otherwise {@code false}.
     *
     * @param sql the SQL to examine
     * @return {@code true} if the sql statement already has a {@code RETURNING} clause, otherwise {@code false}
     * @throws IllegalArgumentException if {@code sql} is {@code null}
     */
    public static boolean hasReturningClause(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        return RETURNING.matcher(sql).matches();
    }

    /**
     * Returns {@code true} if the sql statement has a {@code DELETE}, {@code INSERT}, or {@code UPDATE} command, {@code false} otherwise.
     *
     * @param sql the SQL to examine
     * @return {@code true} if the sql statement has a {@code DELETE}, {@code INSERT}, or {@code UPDATE} command, {@code false} otherwise
     * @throws IllegalArgumentException if {@code sql} is {@code null}
     */
    public static boolean isSupportedCommand(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        return DELETE.matcher(sql).matches() || INSERT.matcher(sql).matches() || UPDATE.matcher(sql).matches();
    }

}
