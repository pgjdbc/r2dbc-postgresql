/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.api;

import reactor.util.annotation.Nullable;

/**
 * Postgres notification received via {@code LISTEN}.
 */
public interface Notification {

    /**
     * Returns name of this notification.
     *
     * @return name of this notification.
     */
    String getName();

    /**
     * Returns the process id of the backend process making this notification.
     *
     * @return process id of the backend process making this notification
     */
    int getProcessId();

    /**
     * Returns additional information from the notifying process. This feature has only been
     * implemented in server versions 9.0 and later, so previous versions will always return {@code null}
     *
     * @return additional information from the notifying process
     */
    @Nullable
    String getParameter();

}
