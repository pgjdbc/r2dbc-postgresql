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

package io.r2dbc.postgresql.authentication;

import reactor.util.annotation.Nullable;

public class UsernameAndPassword {

    private final String username;

    private final @Nullable CharSequence password;

    public UsernameAndPassword(String username, @Nullable CharSequence password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return this.username;
    }

    @Nullable
    public CharSequence getPassword() {
        return this.password;
    }

}
