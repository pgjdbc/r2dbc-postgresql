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

package io.r2dbc.postgresql.exception;

import io.r2dbc.postgresql.PostgresqlAuthenticationFailure;
import io.r2dbc.postgresql.PostgresqlExceptionFactory;
import io.r2dbc.postgresql.PostgresqlServerErrorException;
import io.r2dbc.postgresql.message.backend.AuthenticationGSS;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.Field.FieldType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.SynchronousSink;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

final class PostgresqlExceptionFactoryTest {

    @Test
    void isForwardingMessageNonErrorMessage() {
        SynchronousSink<BackendMessage> sink = createSinkMock();

        BackendMessage message = AuthenticationGSS.INSTANCE;

        PostgresqlExceptionFactory.handleErrorResponse(message, sink);

        verify(sink, times(1)).next(eq(message));
        verify(sink, times(0)).error(isA(PostgresqlAuthenticationFailure.class));
        verify(sink, times(0)).error(isA(PostgresqlServerErrorException.class));
    }

    @Test
    void isCreatingAuthenticationException() {
        List<Field> fields = Arrays.asList(
            new Field(FieldType.CODE, PostgresqlExceptionFactory.AUTH_EXCEPTION_CODE),
            new Field(FieldType.MESSAGE, "error message desc")
        );

        BackendMessage message = new ErrorResponse(fields);

        SynchronousSink<BackendMessage> sink = createSinkMock();

        PostgresqlExceptionFactory.handleErrorResponse(message, sink);

        verify(sink, times(1)).error(isA(PostgresqlAuthenticationFailure.class));
        verify(sink, times(0)).next(eq(message));
    }

    @Test
    void isCreatingPostgresException() {
        List<Field> fields = Arrays.asList(
            new Field(FieldType.CODE, "1234"),
            new Field(FieldType.MESSAGE, "error message desc")
        );

        BackendMessage message = new ErrorResponse(fields);

        SynchronousSink<BackendMessage> sink = createSinkMock();

        PostgresqlExceptionFactory.handleErrorResponse(message, sink);

        verify(sink, times(1)).error(isA(PostgresqlServerErrorException.class));
        verify(sink, times(0)).next(eq(message));
    }

    @SuppressWarnings("unchecked")
    private SynchronousSink<BackendMessage> createSinkMock() {
        return mock(SynchronousSink.class);
    }
}
