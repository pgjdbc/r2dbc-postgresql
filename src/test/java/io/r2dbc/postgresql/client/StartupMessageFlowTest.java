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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.message.backend.AuthenticationMD5Password;
import io.r2dbc.postgresql.message.backend.AuthenticationOk;
import io.r2dbc.postgresql.message.backend.BackendKeyData;
import io.r2dbc.postgresql.message.frontend.PasswordMessage;
import io.r2dbc.postgresql.message.frontend.StartupMessage;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class StartupMessageFlowTest {

    private final AuthenticationHandler authenticationHandler = mock(AuthenticationHandler.class, RETURNS_SMART_NULLS);

    @Test
    void exchangeAuthenticationMessage() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))
                .expectRequest(new PasswordMessage("test-password")).thenRespond(AuthenticationOk.INSTANCE)
                .done()
            .build();
        // @formatter:on

        when(this.authenticationHandler.handle(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))).thenReturn(new PasswordMessage("test-password"));

        StartupMessageFlow
            .exchange("test-application-name", m -> this.authenticationHandler, client, "test-database", "test-username")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void exchangeAuthenticationMessageFail() {
        Client client = TestClient.builder()
            .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))
            .build();

        when(this.authenticationHandler.handle(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))).thenThrow(new IllegalArgumentException());

        StartupMessageFlow
            .exchange("test-application-name", m -> this.authenticationHandler, client, "test-database", "test-username")
            .as(StepVerifier::create)
            .verifyError(IllegalArgumentException.class);
    }

    @Test
    void exchangeAuthenticationOk() {
        Client client = TestClient.builder()
            .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(AuthenticationOk.INSTANCE)
            .build();

        StartupMessageFlow
            .exchange("test-application-name", m -> this.authenticationHandler, client, "test-database", "test-username")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void exchangeAuthenticationOther() {
        Client client = TestClient.builder()
            .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(AuthenticationOk.INSTANCE, new BackendKeyData(100, 200))
            .build();

        StartupMessageFlow
            .exchange("test-application-name", m -> this.authenticationHandler, client, "test-database", "test-username")
            .as(StepVerifier::create)
            .expectNext(new BackendKeyData(100, 200))
            .verifyComplete();
    }

    @Test
    void exchangeNoApplicationName() {
        assertThatIllegalArgumentException().isThrownBy(() -> StartupMessageFlow.exchange(null, m -> this.authenticationHandler, NO_OP, "test-database", "test-username"))
            .withMessage("applicationName must not be null");
    }

    @Test
    void exchangeNoAuthenticationHandlerProvider() {
        assertThatIllegalArgumentException().isThrownBy(() -> StartupMessageFlow.exchange("test-application-name", null, NO_OP, "test-database", "test-username"))
            .withMessage("authenticationHandlerProvider must not be null");
    }

    @Test
    void exchangeNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> StartupMessageFlow.exchange("test-application-name", m -> this.authenticationHandler, null, "test-database", "test-username"))
            .withMessage("client must not be null");
    }

    @Test
    void exchangeNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> StartupMessageFlow.exchange("test-application-name", m -> this.authenticationHandler, NO_OP, "test-database", null))
            .withMessage("username must not be null");
    }

}
