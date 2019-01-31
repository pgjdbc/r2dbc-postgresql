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


import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.r2dbc.postgresql.util.Assert;
import reactor.netty.tcp.SslProvider;
import reactor.util.annotation.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.io.File;
import java.util.function.Supplier;

import static reactor.netty.tcp.SslProvider.DefaultConfigurationType.TCP;

public class SSLConfig {

    private final HostnameVerifier hostnameVerifier;

    private final SSLMode sslMode;

    private final Supplier<SslProvider.Builder> sslProviderSupplier;

    public SSLConfig(SSLMode sslMode, @Nullable Supplier<SslProvider.Builder> sslProviderSupplier, @Nullable HostnameVerifier hostnameVerifier) {
        if (sslMode != SSLMode.DISABLE) {
            Assert.requireNonNull(sslProviderSupplier, "Ssl provider supplier is required for ssl mode " + sslMode);
        }
        if (sslMode.verifyPeerName()) {
            Assert.requireNonNull(hostnameVerifier, "Hostname verifier is required for ssl mode verify-full");
        }
        this.sslMode = sslMode;
        this.sslProviderSupplier = sslProviderSupplier;
        this.hostnameVerifier = hostnameVerifier;
    }

    public static SSLConfig createSslConfig(PostgresqlConnectionConfiguration configuration) {
        if (!configuration.getSsl() || configuration.getSslMode() == SSLMode.DISABLE) {
            return new SSLConfig(SSLMode.DISABLE, () -> null, (hostname, session) -> true);
        }
        HostnameVerifier hostnameVerifier = createHostVerifier(configuration.getSslHostnameVerifier());
        Supplier<SslProvider.Builder> sslProviderBuilder = () -> createSslProvider(configuration);
        return new SSLConfig(configuration.getSslMode(), sslProviderBuilder, hostnameVerifier);
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public SSLMode getSslMode() {
        return sslMode;
    }

    public Supplier<SslProvider.Builder> getSslProviderSupplier() {
        return sslProviderSupplier;
    }

    public SSLConfig mutateMode(SSLMode newMode) {
        return new SSLConfig(
            newMode,
            this.sslProviderSupplier,
            this.hostnameVerifier
        );
    }

    private static HostnameVerifier createHostVerifier(@Nullable String sslHostnameVerifier) {
        if (sslHostnameVerifier == null) {
            return PGHostnameVerifier.INSTANCE;
        }
        try {
            return (HostnameVerifier) Class.forName(sslHostnameVerifier).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static SslProvider.Builder createSslProvider(PostgresqlConnectionConfiguration configuration) {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        if (configuration.getSslMode().verifyCertificate()) {
            String sslRootCert = Assert.requireNonNull(configuration.getSslRootCert(), "SSL mode " + configuration.getSslMode() + " requires sslRootCert parameter");
            sslContextBuilder.trustManager(new File(sslRootCert));
        } else {
            sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
        if (configuration.getSslKey() != null) {
            String sslCert = Assert.requireNonNull(configuration.getSslCert(), "SSL key param requires sslCert param to be present");
            sslContextBuilder.keyManager(new File(sslCert), new File(configuration.getSslKey()), configuration.getSslPassword());
        }
        return SslProvider.builder()
            .sslContext(sslContextBuilder)
            .defaultConfiguration(TCP);
    }
}
