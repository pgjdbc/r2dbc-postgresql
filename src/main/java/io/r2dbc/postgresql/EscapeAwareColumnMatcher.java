/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql;

import org.jspecify.annotations.Nullable;

import java.util.Collection;

/**
 * Matcher utility for column name comparison. Uses case-insensitive comparison by default.
 * Supports name escaping with double-quotes brackets ({@code "sysname"}) to enforce case-sensitive comparison rules.
 */
final class EscapeAwareColumnMatcher {

    public static @Nullable String findColumn(String name, Collection<String> names) {

        for (String s : names) {
            if (matches(name, s)) {
                return s;
            }
        }

        return null;
    }

    private static boolean matches(String o1, String o2) {

        boolean exactMatch = false;

        if (o1.startsWith("\"") && o1.endsWith("\"")) {
            exactMatch = true;
            o1 = o1.substring(1, o1.length() - 1);
        }

        if (o2.startsWith("\"") && o2.endsWith("\"")) {
            exactMatch = true;
            o2 = o2.substring(1, o2.length() - 1);
        }

        return exactMatch ? o1.equals(o2) : o1.equalsIgnoreCase(o2);
    }

}
