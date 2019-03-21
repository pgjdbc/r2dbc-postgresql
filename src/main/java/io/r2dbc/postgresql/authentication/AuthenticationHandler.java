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

package io.r2dbc.postgresql.authentication;

import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import reactor.util.annotation.Nullable;

/**
 * A handler for {@link AuthenticationMessage}s.  Responsible for handling the entire interaction for a given authentication style.
 */
public interface AuthenticationHandler {

    /**
     * Handle the incoming authentication message.  Note that implementations do not need handle the {@link AuthenticationMessage} type as callers are expected to have handled it directly and are
     * passing a more specific type.
     *
     * @param message the message to handle
     * @return the next outbound message to send
     * @throws IllegalArgumentException if {@code message} is {@code null}
     */
    @Nullable
    FrontendMessage handle(AuthenticationMessage message);

}
