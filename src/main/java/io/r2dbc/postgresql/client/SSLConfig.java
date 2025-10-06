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

package io.r2dbc.postgresql.client;

import io.netty.handler.ssl.SslContext;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.SocketAddress;
import java.util.function.Function;
import java.util.function.Supplier;

public final class SSLConfig {

    @Nullable
    private final HostnameVerifier hostnameVerifier;

    private final SSLNegotiation sslNegotiation;

    private final SSLMode sslMode;

    @Nullable
    private final Supplier<SslContext> sslProvider;

    private final Function<SSLEngine, SSLEngine> sslEngineCustomizer;

    private final Function<SocketAddress, SSLParameters> sslParametersFactory;

    public SSLConfig(SSLMode sslMode, @Nullable Supplier<SslContext> sslProvider, @Nullable HostnameVerifier hostnameVerifier) {
        this(SSLNegotiation.POSTGRES, sslMode, sslProvider, Function.identity(), it -> new SSLParameters(), hostnameVerifier);
    }

    public SSLConfig(SSLNegotiation sslNegotiation, SSLMode sslMode, @Nullable Supplier<SslContext> sslProvider, Function<SSLEngine, SSLEngine> sslEngineCustomizer, Function<SocketAddress,
                         SSLParameters> sslParametersFactory,
                     @Nullable HostnameVerifier hostnameVerifier) {
        this.sslNegotiation = sslNegotiation;
        if (sslMode != SSLMode.DISABLE) {
            Assert.requireNonNull(sslProvider, "SslContext provider is required for ssl mode " + sslMode);
        }
        if (sslMode.verifyPeerName()) {
            Assert.requireNonNull(hostnameVerifier, "Hostname verifier is required for ssl mode verify-full");
        }
        this.sslMode = sslMode;
        this.sslProvider = sslProvider;
        this.sslEngineCustomizer = sslEngineCustomizer;
        this.sslParametersFactory = sslParametersFactory;
        this.hostnameVerifier = hostnameVerifier;
    }

    public static SSLConfig disabled() {
        return new SSLConfig(SSLMode.DISABLE, null, (hostname, session) -> true);
    }

    HostnameVerifier getHostnameVerifier() {
        return this.hostnameVerifier;
    }

    @SuppressWarnings("deprecation")
    public boolean isDirectSsl() {
        return getSslMode() == SSLMode.TUNNEL || getSslNegotiation() == SSLNegotiation.DIRECT || getSslNegotiation() == SSLNegotiation.TUNNEL;
    }

    public SSLNegotiation getSslNegotiation() {
        return this.sslNegotiation;
    }

    public SSLMode getSslMode() {
        return this.sslMode;
    }

    public Supplier<SslContext> getSslProvider() {
        if (this.sslProvider == null) {
            throw new IllegalStateException("SSL Mode disabled. SslContext provider not available");
        }
        return this.sslProvider;
    }

    public Function<SSLEngine, SSLEngine> getSslEngineCustomizer() {
        return this.sslEngineCustomizer;
    }

    public Function<SocketAddress, SSLParameters> getSslParametersFactory() {
        return this.sslParametersFactory;
    }

    public SSLConfig mutateMode(SSLMode newMode) {
        return new SSLConfig(
            this.sslNegotiation,
            newMode,
            this.sslProvider,
            this.sslEngineCustomizer,
            this.sslParametersFactory,
            this.hostnameVerifier
        );
    }

    public static boolean isValidSniHostname(String input) {
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (isLabelSeparator(c)) {
                continue;
            }
            if (isNonLDHAsciiCodePoint(c)) {
                return false;
            }
        }
        return !input.endsWith(".");
    }

    //
    // LDH stands for "letter/digit/hyphen", with characters restricted to the
    // 26-letter Latin alphabet <A-Z a-z>, the digits <0-9>, and the hyphen
    // <->.
    // Non LDH refers to characters in the ASCII range, but which are not
    // letters, digits or the hypen.
    //
    // non-LDH = 0..0x2C, 0x2E..0x2F, 0x3A..0x40, 0x5B..0x60, 0x7B..0x7F
    //
    private static boolean isNonLDHAsciiCodePoint(char ch) {
        return (0x0000 <= ch && ch <= 0x002C) ||
            (0x002E <= ch && ch <= 0x002F) ||
            (0x003A <= ch && ch <= 0x0040) ||
            (0x005B <= ch && ch <= 0x0060) ||
            (0x007B <= ch && ch <= 0x007F);
    }

    //
    // to check if a character is a label separator, i.e. a dot character.
    //
    private static boolean isLabelSeparator(char c) {
        return (c == '.' || c == '\u3002' || c == '\uFF0E' || c == '\uFF61');
    }

}
