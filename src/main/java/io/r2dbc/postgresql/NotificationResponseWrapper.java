/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.message.backend.NotificationResponse;

/**
 * {@link Notification} wrapper for {@link NotificationResponse}.
 */
final class NotificationResponseWrapper implements Notification {

    private final NotificationResponse notification;

    NotificationResponseWrapper(NotificationResponse notification) {
        this.notification = notification;
    }

    @Override
    public String getName() {
        return this.notification.getName();
    }

    @Override
    public int getProcessId() {
        return this.notification.getProcessId();
    }

    @Override
    public String getParameter() {
        return this.notification.getPayload();
    }

    @Override
    public String toString() {
        return "NotificationResponseWrapper{" +
            "name=" + getName() +
            "processId=" + getProcessId() +
            "parameter=" + getParameter() +
            '}';
    }
}
