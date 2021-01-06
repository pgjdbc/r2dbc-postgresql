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

import io.r2dbc.postgresql.message.backend.AuthenticationCleartextPassword;
import io.r2dbc.postgresql.message.backend.AuthenticationGSS;
import io.r2dbc.postgresql.message.backend.AuthenticationMD5Password;
import io.r2dbc.postgresql.message.frontend.PasswordMessage;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PasswordAuthenticationHandler}.
 */
final class PasswordAuthenticationHandlerUnitTests {

    @Test
    void constructorNoPassword() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PasswordAuthenticationHandler(null, "test-username"))
            .withMessage("password must not be null");
    }

    @Test
    void constructorNoUsername() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PasswordAuthenticationHandler("test-password", null))
            .withMessage("username must not be null");
    }

    @Test
    void handleAuthenticationCleartextPassword() {
        PasswordAuthenticationHandler handler = new PasswordAuthenticationHandler("test-password", "test-username");
        AuthenticationCleartextPassword message = AuthenticationCleartextPassword.INSTANCE;

        assertThat(handler.handle(message)).isEqualTo(new PasswordMessage("test-password"));
    }

    @Test
    void handleAuthenticationMD5Password() {
        PasswordAuthenticationHandler handler = new PasswordAuthenticationHandler("test-password", "test-username");
        AuthenticationMD5Password message = new AuthenticationMD5Password(TEST.buffer(4).writeInt(100));

        assertThat(handler.handle(message)).isEqualTo(new PasswordMessage("md55e9836cdb369d50e3bc7d127e88b4804"));
    }

    @Test
    void handleNoMessage() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PasswordAuthenticationHandler("test-username", "test-password").handle(null))
            .withMessage("message must not be null");
    }

    @Test
    void handleUnknown() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PasswordAuthenticationHandler("test-username", "test-password").handle(AuthenticationGSS.INSTANCE))
            .withMessage("Cannot handle AuthenticationGSS message");
    }

}
