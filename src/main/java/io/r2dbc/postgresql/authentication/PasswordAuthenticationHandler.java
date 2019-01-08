/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.authentication;

import io.r2dbc.postgresql.message.backend.AuthenticationCleartextPassword;
import io.r2dbc.postgresql.message.backend.AuthenticationMD5Password;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.PasswordMessage;
import io.r2dbc.postgresql.util.Assert;

/**
 * An implementation of {@link AuthenticationHandler} that handles {@link AuthenticationCleartextPassword} and {@link AuthenticationMD5Password} messages.
 */
public final class PasswordAuthenticationHandler implements AuthenticationHandler {

    private final String password;

    private final String username;

    /**
     * Creates a new handler.
     *
     * @param password the password to use for authentication
     * @param username the username to use for authentication
     * @throws IllegalArgumentException if {@code password} or {@code user} is {@code null}
     */
    public PasswordAuthenticationHandler(String password, String username) {
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.username = Assert.requireNonNull(username, "username must not be null");
    }

    /**
     * Returns whether this {@link AuthenticationHandler} can support authentication for a given authentication message response.
     *
     * @param message the message to inspect
     * @return whether this {@link AuthenticationHandler} can support authentication for a given authentication message response
     * @throws IllegalArgumentException if {@code message} is {@code null}
     */
    public static boolean supports(AuthenticationMessage message) {
        Assert.requireNonNull(message, "message must not be null");

        return message instanceof AuthenticationCleartextPassword || message instanceof AuthenticationMD5Password;
    }

    @Override
    public FrontendMessage handle(AuthenticationMessage message) {
        Assert.requireNonNull(message, "message must not be null");

        if (message instanceof AuthenticationCleartextPassword) {
            return handleAuthenticationClearTextPassword();
        } else if (message instanceof AuthenticationMD5Password) {
            return handleAuthenticationMD5Password((AuthenticationMD5Password) message);
        } else {
            throw new IllegalArgumentException(String.format("Cannot handle %s message", message.getClass().getSimpleName()));
        }
    }

    private PasswordMessage handleAuthenticationClearTextPassword() {
        return new PasswordMessage(this.password);
    }

    private FrontendMessage handleAuthenticationMD5Password(AuthenticationMD5Password message) {
        String shadow = new FluentMessageDigest("md5")
            .update("%s%s", this.password, this.username)
            .digest();

        String transfer = new FluentMessageDigest("md5")
            .update(shadow)
            .update(message.getSalt())
            .digest();

        return new PasswordMessage(String.format("md5%s", transfer));
    }

}
