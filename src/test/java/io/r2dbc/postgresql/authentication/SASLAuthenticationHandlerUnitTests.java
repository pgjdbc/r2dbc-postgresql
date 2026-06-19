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

package io.r2dbc.postgresql.authentication;

import io.r2dbc.postgresql.client.ChannelBindingMode;
import io.r2dbc.postgresql.client.ConnectionContext;
import io.r2dbc.postgresql.message.backend.AuthenticationSASL;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SASLAuthenticationHandler}.
 */
class SASLAuthenticationHandlerUnitTests {

    AuthenticationSASL BOTH = new AuthenticationSASL(Arrays.asList("SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"));

    AuthenticationSASL NON_PLUS_ONLY = new AuthenticationSASL(Collections.singletonList("SCRAM-SHA-256"));

    @Test
    void constructorNoChannelBinding() {
        assertThat(new SASLAuthenticationHandler("test-password", "test-username", new ConnectionContext())).isNotNull();
    }

    @Test
    void requireChannelBindingWithoutSslFails() {
        SASLAuthenticationHandler handler = new SASLAuthenticationHandler("test-password", "test-username", new ConnectionContext(), ChannelBindingMode.REQUIRE);

        assertThatExceptionOfType(R2dbcPermissionDeniedException.class).isThrownBy(() -> handler.handle(BOTH))
            .withMessageContaining("not using SSL");
    }

    @Test
    void requireChannelBindingWithoutPlusMechanismFails() {
        ConnectionContext context = new ConnectionContext().withSslSession(SASLAuthenticationHandlerUnitTests::validSslSession);
        SASLAuthenticationHandler handler = new SASLAuthenticationHandler("test-password", "test-username", context, ChannelBindingMode.REQUIRE);

        assertThatExceptionOfType(R2dbcPermissionDeniedException.class).isThrownBy(() -> handler.handle(NON_PLUS_ONLY))
            .withMessageContaining("SCRAM-SHA-256-PLUS");
    }

    @Test
    void preferChannelBindingFallsBackToNonPlusWithoutSsl() {
        SASLAuthenticationHandler handler = new SASLAuthenticationHandler("test-password", "test-username", new ConnectionContext(), ChannelBindingMode.PREFER);

        FrontendMessage response = handler.handle(BOTH);

        assertThat(response.toString()).contains("name='SCRAM-SHA-256'");
    }

    @Test
    void disableChannelBindingDoesNotUsePlusEvenWithSsl() {
        ConnectionContext context = new ConnectionContext().withSslSession(SASLAuthenticationHandlerUnitTests::validSslSession);
        SASLAuthenticationHandler handler = new SASLAuthenticationHandler("test-password", "test-username", context, ChannelBindingMode.DISABLE);

        FrontendMessage response = handler.handle(BOTH);

        assertThat(response.toString()).contains("name='SCRAM-SHA-256'");
    }

    private static SSLSession validSslSession() {
        SSLSession sslSession = mock(SSLSession.class);
        when(sslSession.isValid()).thenReturn(true);
        return sslSession;
    }

}
