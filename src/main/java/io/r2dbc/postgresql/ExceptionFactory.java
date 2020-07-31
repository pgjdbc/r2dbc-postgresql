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

import io.r2dbc.postgresql.api.ErrorDetails;
import io.r2dbc.postgresql.api.PostgresqlException;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTransientException;
import reactor.core.publisher.SynchronousSink;

/**
 * Factory for Postgres-specific {@link R2dbcException}s.
 */
final class ExceptionFactory {

    public static final ExceptionFactory INSTANCE = new ExceptionFactory("");

    private final String sql;

    private ExceptionFactory(String sql) {
        this.sql = sql;
    }

    /**
     * Create a {@link ExceptionFactory} associated with a SQL query.
     *
     * @param sql
     * @return
     */
    static ExceptionFactory withSql(String sql) {
        return new ExceptionFactory(sql);
    }

    /**
     * Create a {@link R2dbcException} from an {@link ErrorResponse}.
     *
     * @param response the response that contains the error details.
     * @param sql      underlying SQL.
     * @return the {@link R2dbcException}.
     * @see ErrorResponse
     */
    private static R2dbcException createException(ErrorResponse response, String sql) {

        ErrorDetails errorDetails = new ErrorDetails(response.getFields());

        switch (errorDetails.getCode()) {
            case "42501":
                return new PostgresqlPermissionDeniedException(errorDetails);
            case "40000":
                return new PostgresqlRollbackException(errorDetails);
            case "28000":
            case "28P01":
                return new PostgresqlAuthenticationFailure(errorDetails);

        }

        String codeClass = errorDetails.getCode().length() > 2 ? errorDetails.getCode().substring(0, 2) : "99";

        switch (codeClass) {
            case "03": // SQL Statement Not Yet Complete
            case "42": // Syntax Error or Access Rule Violation
            case "22": // Data Exception
            case "26": // Invalid SQL Statement Name
                return new PostgresqlBadGrammarException(errorDetails, sql);
            case "08": // Connection Exception
                return new PostgresqlNonTransientResourceException(errorDetails);
            case "21": // Cardinality Violation
            case "23": // Integrity Constraint Violation
            case "27": // Integrity Constraint Violation
                return new PostgresqlDataIntegrityViolationException(errorDetails);
            case "28": // Invalid Authorization Specification
                return new PostgresqlPermissionDeniedException(errorDetails);
            case "40": // Invalid Authorization Specification
                return new PostgresqlTransientException(errorDetails);
        }

        return new PostgresqlNonTransientResourceException(errorDetails);
    }

    /**
     * Handle {@link BackendMessage}s and inspect for {@link ErrorResponse} to emit a {@link R2dbcException}.
     *
     * @param message the message.
     * @param sink    the outbound sink.
     */
    void handleErrorResponse(BackendMessage message, SynchronousSink<BackendMessage> sink) {

        if (message instanceof ErrorResponse) {
            sink.error(createException((ErrorResponse) message, this.sql));
        } else {
            sink.next(message);
        }
    }

    /**
     * Postgres-specific {@link R2dbcBadGrammarException}.
     */
    static final class PostgresqlBadGrammarException extends R2dbcBadGrammarException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlBadGrammarException(ErrorDetails errorDetails, String offendingSql) {
            super(errorDetails.getMessage(), errorDetails.getCode(), 0, offendingSql);
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcDataIntegrityViolationException}.
     */
    static final class PostgresqlDataIntegrityViolationException extends R2dbcDataIntegrityViolationException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlDataIntegrityViolationException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcNonTransientResourceException}.
     */
    static final class PostgresqlNonTransientResourceException extends R2dbcNonTransientResourceException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlNonTransientResourceException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcPermissionDeniedException}.
     */
    static final class PostgresqlPermissionDeniedException extends R2dbcPermissionDeniedException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlPermissionDeniedException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcRollbackException}.
     */
    static final class PostgresqlRollbackException extends R2dbcRollbackException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlRollbackException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcTransientException}.
     */
    static final class PostgresqlTransientException extends R2dbcTransientException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlTransientException(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Postgres-specific {@link R2dbcPermissionDeniedException}.
     */
    static final class PostgresqlAuthenticationFailure extends R2dbcPermissionDeniedException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        PostgresqlAuthenticationFailure(ErrorDetails errorDetails) {
            super(errorDetails.getMessage(), errorDetails.getCode());
            this.errorDetails = errorDetails;
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

}
