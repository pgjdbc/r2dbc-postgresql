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

import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.util.function.Supplier;

public final class SSLConfig {

    @Nullable
    private final HostnameVerifier hostnameVerifier;

    private final SSLMode sslMode;

    @Nullable
    private final Supplier<SslProvider> sslProvider;

    public SSLConfig(SSLMode sslMode, @Nullable Supplier<SslProvider> sslProvider, @Nullable HostnameVerifier hostnameVerifier) {
        if (sslMode != SSLMode.DISABLE) {
            Assert.requireNonNull(sslProvider, "Ssl provider is required for ssl mode " + sslMode);
        }
        if (sslMode.verifyPeerName()) {
            Assert.requireNonNull(hostnameVerifier, "Hostname verifier is required for ssl mode verify-full");
        }
        this.sslMode = sslMode;
        this.sslProvider = sslProvider;
        this.hostnameVerifier = hostnameVerifier;
    }

    public static SSLConfig disabled() {
        return new SSLConfig(SSLMode.DISABLE, null, (hostname, session) -> true);
    }

    HostnameVerifier getHostnameVerifier() {
        return this.hostnameVerifier;
    }

    public SSLMode getSslMode() {
        return this.sslMode;
    }

    public Supplier<SslProvider> getSslProvider() {
        if (this.sslProvider == null) {
            throw new IllegalStateException("SSL Mode disabled. SslProvider not available");
        }
        return this.sslProvider;
    }

    public SSLConfig mutateMode(SSLMode newMode) {
        return new SSLConfig(
            newMode,
            this.sslProvider,
            this.hostnameVerifier
        );
    }

}
