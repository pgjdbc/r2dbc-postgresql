/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import reactor.core.publisher.SynchronousSink;

import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;

public final class PostgresqlExceptionFactory {

    static final String AUTH_EXCEPTION_CODE = "28P01";

    private PostgresqlExceptionFactory() {
    }

    public static void handleErrorResponse(BackendMessage message, SynchronousSink<BackendMessage> sink) {
        if (!(message instanceof ErrorResponse)) {
            sink.next(message);
            return;
        }

        ErrorResponse error = (ErrorResponse) message;

        boolean isAuthError = error.getFields()
            .stream()
            .anyMatch(field ->
                CODE.equals(field.getType()) &&
                    AUTH_EXCEPTION_CODE.equals(field.getValue())
            );

        if (isAuthError) {
            sink.error(PostgresqlAuthenticationFailure.toException(error));
            return;
        }

        sink.error(PostgresqlServerErrorException.toException(error));
    }

}
