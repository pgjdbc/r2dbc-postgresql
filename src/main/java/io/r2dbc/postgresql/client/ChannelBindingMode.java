/*
 * Copyright 2026 the original author or authors.
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

import java.util.Arrays;

/**
 * Channel binding preference for SCRAM (SASL) authentication. Mirrors libpq's {@code channel_binding} connection parameter.
 *
 * @since 1.1.2
 */
public enum ChannelBindingMode {

    /**
     * Channel binding is never used, even if the connection is encrypted and the server offers a channel-binding mechanism.
     */
    DISABLE("disable"),

    /**
     * Channel binding is used if the connection is encrypted and the server offers a channel-binding SCRAM mechanism (e.g. {@code SCRAM-SHA-256-PLUS}); otherwise authentication falls back to a
     * non-bound SCRAM mechanism. This is the default and matches the historic driver behavior.
     */
    PREFER("prefer"),

    /**
     * Channel binding is required. Authentication fails if the connection is not encrypted or the server does not offer a channel-binding SCRAM mechanism. Prevents a man-in-the-middle from silently
     * downgrading the negotiation to a non-bound mechanism.
     */
    REQUIRE("require");

    private final String value;

    ChannelBindingMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    /**
     * Resolve a {@link ChannelBindingMode} from its libpq-style value (e.g. {@code require}) or enum name (e.g. {@code REQUIRE}).
     *
     * @param value the value to resolve
     * @return the resolved {@link ChannelBindingMode}
     * @throws IllegalArgumentException if the value does not map to a known mode
     */
    public static ChannelBindingMode fromValue(String value) {
        for (ChannelBindingMode mode : values()) {
            if (mode.value.equalsIgnoreCase(value) || mode.name().equalsIgnoreCase(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Invalid channel binding mode value: " + value + ". Supported values are: " + Arrays.toString(values()));
    }
}
