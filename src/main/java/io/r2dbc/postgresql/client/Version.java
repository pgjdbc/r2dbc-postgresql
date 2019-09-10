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

package io.r2dbc.postgresql.client;

import java.text.NumberFormat;
import java.text.ParsePosition;

/**
 * Postgres version information.
 */
public final class Version {

    private final String version;

    private final Integer versionNumber;

    public Version(String version) {
        this(version, parseServerVersionStr(version));
    }

    public Version(String version, int versionNumber) {
        this.version = version;
        this.versionNumber = versionNumber;
    }

    /**
     * <p>Note that there's no requirement for this to be numeric or of the form x.y.z. PostgreSQL
     * development releases usually have the format x.ydevel e.g. 9.4devel; betas usually x.ybetan
     * e.g. 9.4beta1. The --with-extra-version configure option may add an arbitrary string to this.</p>
     * <p>Don't use this string for logic, only use it when displaying the server version to the user.
     * * Prefer {@link #getVersionNumber()} for all logic purposes.</p>
     *
     * @return
     */
    public String getVersion() {
        return version;
    }

    /**
     * Server version number.
     *
     * @return
     */
    public int getVersionNumber() {
        return versionNumber;
    }

    @Override
    public String toString() {
        return "Version{" +
            "version='" + version + '\'' +
            ", versionNumber=" + versionNumber +
            '}';
    }

    /**
     * Attempt to parse the server version string into an XXYYZZ form version number.
     * <ul>
     * <li>Returns 0 if the version could not be parsed.</li>
     * <li>Returns minor version 0 if the minor version could not be determined, e.g. devel or beta
     * releases.</li>
     * </ul>
     *
     * <p>If a single major part like 90400 is passed, it's assumed to be a pre-parsed version and
     * returned verbatim. (Anything equal to or greater than 10000 is presumed to be this form).</p>
     *
     * <p>The yy or zz version parts may be larger than 99. A NumberFormatException is thrown if a
     * version part is out of range.</p>
     *
     * @param serverVersion server version in a XXYYZZ form
     * @return server version in number form
     */
    static int parseServerVersionStr(String serverVersion) throws NumberFormatException {
        NumberFormat numformat = NumberFormat.getIntegerInstance();
        numformat.setGroupingUsed(false);
        ParsePosition parsepos = new ParsePosition(0);

        if (serverVersion == null) {
            return 0;
        }

        int[] parts = new int[3];
        int versionParts;
        for (versionParts = 0; versionParts < 3; versionParts++) {
            Number part = (Number) numformat.parseObject(serverVersion, parsepos);
            if (part == null) {
                break;
            }
            parts[versionParts] = part.intValue();
            if (parsepos.getIndex() == serverVersion.length()
                || serverVersion.charAt(parsepos.getIndex()) != '.') {
                break;
            }
            // Skip .
            parsepos.setIndex(parsepos.getIndex() + 1);
        }
        versionParts++;

        if (parts[0] >= 10000) {
            if (parsepos.getIndex() == serverVersion.length() && versionParts == 1) {
                return parts[0];
            } else {
                throw new NumberFormatException(
                    "First major-version part equal to or greater than 10000 in invalid version string: "
                        + serverVersion);
            }
        }

        // Allow for versions with greater than 3 parts.

        if (versionParts >= 3) {
            if (parts[1] > 99) {
                throw new NumberFormatException(
                    "Unsupported second part of major version > 99 in invalid version string: "
                        + serverVersion);
            }
            if (parts[2] > 99) {
                throw new NumberFormatException(
                    "Unsupported second part of minor version > 99 in invalid version string: "
                        + serverVersion);
            }
            return (parts[0] * 100 + parts[1]) * 100 + parts[2];
        }
        if (versionParts == 2) {
            if (parts[0] >= 10) {
                return parts[0] * 100 * 100 + parts[1];
            }
            if (parts[1] > 99) {
                throw new NumberFormatException(
                    "Unsupported second part of major version > 99 in invalid version string: "
                        + serverVersion);
            }
            return (parts[0] * 100 + parts[1]) * 100;
        }
        if (versionParts == 1) {
            if (parts[0] >= 10) {
                return parts[0] * 100 * 100;
            }
        }
        return 0; /* unknown */
    }
}
