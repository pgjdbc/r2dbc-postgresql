/*
 * Copyright 2022 the original author or authors.
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

package io.r2dbc.postgresql.client;

import java.util.HashMap;
import java.util.TimeZone;

/**
 * Utility to handle timezone-related parsing.
 */
final class TimeZoneUtils {

    private static final char[][] NUMBERS = new char[64][];

    private static final HashMap<String, TimeZone> GMT_ZONES = new HashMap<String, TimeZone>();

    static {

        // The expected maximum value is 60 (seconds), so 64 is used "just in case"
        for (int i = 0; i < NUMBERS.length; i++) {
            NUMBERS[i] = ((i < 10 ? "0" : "") + Integer.toString(i)).toCharArray();
        }

        // Backend's gmt-3 means GMT+03 in Java. Here a map is created so gmt-3 can be converted to
        // java TimeZone
        for (int i = -12; i <= 14; i++) {
            TimeZone timeZone;
            String pgZoneName;
            if (i == 0) {
                timeZone = TimeZone.getTimeZone("GMT");
                pgZoneName = "GMT";
            } else {
                timeZone = TimeZone.getTimeZone("GMT" + (i <= 0 ? "+" : "-") + Math.abs(i));
                pgZoneName = "GMT" + (i >= 0 ? "+" : "-");
            }

            if (i == 0) {
                GMT_ZONES.put(pgZoneName, timeZone);
                continue;
            }
            GMT_ZONES.put(pgZoneName + Math.abs(i), timeZone);
            GMT_ZONES.put(pgZoneName + new String(NUMBERS[Math.abs(i)]), timeZone);
        }
    }

    /**
     * Converts backend's TimeZone parameter to java format.
     * Notable difference: backend's gmt-3 is GMT+03 in Java.
     *
     * @param timeZone time zone to use
     * @return java TimeZone
     */
    public static TimeZone parseBackendTimeZone(String timeZone) {

        if (timeZone.startsWith("GMT")) {
            TimeZone tz = GMT_ZONES.get(timeZone);
            if (tz != null) {
                return tz;
            }
        }
        return TimeZone.getTimeZone(timeZone);
    }

    /**
     * Convert Java time zone to postgres time zone. All others stay the same except that GMT+nn
     * changes to GMT-nn and vise versa.
     * If you provide GMT+/-nn postgres uses POSIX rules which has a positive sign for west of Greenwich
     * JAVA uses ISO rules which the positive sign is east of Greenwich
     * To make matters more interesting postgres will always report in ISO
     *
     * @param timeZone time zone to use
     * @return The current JVM time zone in postgresql format.
     */
    public static String createPostgresTimeZone(TimeZone timeZone) {
        String tz = timeZone.getID();
        if (tz.length() <= 3 || !tz.startsWith("GMT")) {
            return tz;
        }
        char sign = tz.charAt(3);
        String start;
        switch (sign) {
            case '+':
                start = "GMT-";
                break;
            case '-':
                start = "GMT+";
                break;
            default:
                // unknown type
                return tz;
        }

        return start + tz.substring(4);
    }

}
