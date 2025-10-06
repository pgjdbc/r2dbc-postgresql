/*
 * Copyright 2025 the original author or authors.
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

import javax.net.ssl.SSLParameters;
import java.util.Arrays;

/**
 * @since 1.1
 */
public enum SSLNegotiation {
    /**
     * Use the Postgres protocol to negotiate SSL requiring more roundtrips but working with all Postgres server versions.
     */
    POSTGRES("postgres"),

    /**
     * Use direct SSL ALPN negotiation using the {@code postgresql} {@link SSLParameters#setApplicationProtocols(String[]) protocol}.
     */
    DIRECT("direct"),

    /**
     * Use a direct SSL negotiation assuming the server endpoint is an SSL terminator with a Postgres instance behind it. Replacement for {@link SSLMode#TUNNEL}.
     */
    TUNNEL("tunnel");

    private final String value;

    SSLNegotiation(String value) {
        this.value = value;
    }

    public static SSLNegotiation fromValue(String sslNegotiationString) {
        for (SSLNegotiation sslMode : values()) {
            if (sslMode.value.equalsIgnoreCase(sslNegotiationString) || sslMode.name().equalsIgnoreCase(sslNegotiationString)) {
                return sslMode;
            }
        }
        throw new IllegalArgumentException("Invalid ssl negotiation value: " + sslNegotiationString + ". Supported values are: " + Arrays.toString(values()));
    }

    public String value() {
        return this.value;
    }

}
