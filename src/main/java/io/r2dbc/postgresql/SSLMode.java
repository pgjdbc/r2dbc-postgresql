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

package io.r2dbc.postgresql;

public enum SSLMode {
    /**
     * I don't care about security and don't want to pay the overhead for encryption
     */
    DISABLE("disable"),

    /**
     * I don't care about security but will pay the overhead for encryption if the server insists on it
     */
    ALLOW("allow"),

    /**
     * I don't care about encryption but will pay the overhead of encryption if the server supports it
     */
    PREFER("prefer"),

    /**
     * I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want.
     */
    REQUIRE("require"),

    /**
     * I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust.
     */
    VERIFY_CA("verify-ca"),

    /**
     * I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify.
     */
    VERIFY_FULL("verify-full");

    private final String value;

    SSLMode(String value) {
        this.value = value;
    }

    public boolean requireSsl() {
        return this == REQUIRE || this == VERIFY_CA || this == VERIFY_FULL;
    }

    public boolean startSsl() {
        return this != DISABLE && this != ALLOW;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean verifyCertificate() {
        return this == VERIFY_CA || this == VERIFY_FULL;
    }

    public boolean verifyPeerName() {
        return this == VERIFY_FULL;
    }
}
