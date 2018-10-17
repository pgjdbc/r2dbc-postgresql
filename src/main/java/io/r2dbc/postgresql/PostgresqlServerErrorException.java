/*
 * Copyright 2017-2018 the original author or authors.
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
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.Field.FieldType;
import io.r2dbc.spi.R2dbcException;
import reactor.core.publisher.SynchronousSink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.COLUMN_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CONSTRAINT_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.DATA_TYPE_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.DETAIL;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.FILE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.HINT;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.INTERNAL_POSITION;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.INTERNAL_QUERY;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.LINE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.MESSAGE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.POSITION;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.ROUTINE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SCHEMA_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SEVERITY_LOCALIZED;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SEVERITY_NON_LOCALIZED;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.TABLE_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.WHERE;

/**
 * An exception that represents a server error.  This exception is a direct translation of the {@link ErrorResponse} message.
 */
public final class PostgresqlServerErrorException extends R2dbcException {

    private final String code;

    private final String columnName;

    private final String constraintName;

    private final String dataTypeName;

    private final String detail;

    private final String file;

    private final String hint;

    private final String internalPosition;

    private final String internalQuery;

    private final String line;

    private final String message;

    private final String position;

    private final String routine;

    private final String schemaName;

    private final String severityLocalized;

    private final String severityNonLocalized;

    private final String tableName;

    private final String where;


    /**
     * Creates a new exception.
     *
     * @param fields the fields to be used to populate the exception
     * @throws NullPointerException if {@code fields} is {@code null}
     */
    public PostgresqlServerErrorException(List<Field> fields) {
        this(convertToMap(fields));
    }

    private PostgresqlServerErrorException(Map<FieldType, String> fields) {
        super(fields.get(MESSAGE), fields.get(CODE));

        this.code = fields.get(CODE);
        this.columnName = fields.get(COLUMN_NAME);
        this.constraintName = fields.get(CONSTRAINT_NAME);
        this.dataTypeName = fields.get(DATA_TYPE_NAME);
        this.detail = fields.get(DETAIL);
        this.file = fields.get(FILE);
        this.hint = fields.get(HINT);
        this.internalPosition = fields.get(INTERNAL_POSITION);
        this.internalQuery = fields.get(INTERNAL_QUERY);
        this.line = fields.get(LINE);
        this.message = fields.get(MESSAGE);
        this.position = fields.get(POSITION);
        this.routine = fields.get(ROUTINE);
        this.schemaName = fields.get(SCHEMA_NAME);
        this.severityLocalized = fields.get(SEVERITY_LOCALIZED);
        this.severityNonLocalized = fields.get(SEVERITY_NON_LOCALIZED);
        this.tableName = fields.get(TABLE_NAME);
        this.where = fields.get(WHERE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PostgresqlServerErrorException that = (PostgresqlServerErrorException) o;
        return Objects.equals(this.code, that.code) &&
            Objects.equals(this.columnName, that.columnName) &&
            Objects.equals(this.constraintName, that.constraintName) &&
            Objects.equals(this.dataTypeName, that.dataTypeName) &&
            Objects.equals(this.detail, that.detail) &&
            Objects.equals(this.file, that.file) &&
            Objects.equals(this.hint, that.hint) &&
            Objects.equals(this.internalPosition, that.internalPosition) &&
            Objects.equals(this.internalQuery, that.internalQuery) &&
            Objects.equals(this.line, that.line) &&
            Objects.equals(this.message, that.message) &&
            Objects.equals(this.position, that.position) &&
            Objects.equals(this.routine, that.routine) &&
            Objects.equals(this.schemaName, that.schemaName) &&
            Objects.equals(this.severityLocalized, that.severityLocalized) &&
            Objects.equals(this.severityNonLocalized, that.severityNonLocalized) &&
            Objects.equals(this.tableName, that.tableName) &&
            Objects.equals(this.where, that.where);
    }

    /**
     * Returns the value of the {@link FieldType#CODE} field.
     *
     * @return the value of the {@link FieldType#CODE} field
     */
    public String getCode() {
        return this.code;
    }

    /**
     * Returns the value of the {@link FieldType#COLUMN_NAME} field.
     *
     * @return the value of the {@link FieldType#COLUMN_NAME} field
     */
    public Optional<String> getColumnName() { return Optional.ofNullable(this.columnName); }

    /**
     * Returns the value of the {@link FieldType#CONSTRAINT_NAME} field.
     *
     * @return the value of the {@link FieldType#CONSTRAINT_NAME} field
     */
    public String getConstraintName() {
        return this.constraintName;
    }

    /**
     * Returns the value of the {@link FieldType#DATA_TYPE_NAME} field.
     *
     * @return the value of the {@link FieldType#DATA_TYPE_NAME} field
     */
    public String getDataTypeName() {
        return this.dataTypeName;
    }

    /**
     * Returns the value of the {@link FieldType#DETAIL} field.
     *
     * @return the value of the {@link FieldType#DETAIL} field
     */
    public String getDetail() {
        return this.detail;
    }

    /**
     * Returns the value of the {@link FieldType#FILE} field.
     *
     * @return the value of the {@link FieldType#FILE} field
     */
    public String getFile() {
        return this.file;
    }

    /**
     * Returns the value of the {@link FieldType#HINT} field.
     *
     * @return the value of the {@link FieldType#HINT} field
     */
    public String getHint() {
        return this.hint;
    }

    /**
     * Returns the value of the {@link FieldType#INTERNAL_POSITION} field.
     *
     * @return the value of the {@link FieldType#INTERNAL_POSITION} field
     */
    public String getInternalPosition() {
        return this.internalPosition;
    }

    /**
     * Returns the value of the {@link FieldType#INTERNAL_QUERY} field.
     *
     * @return the value of the {@link FieldType#INTERNAL_QUERY} field
     */
    public String getInternalQuery() {
        return this.internalQuery;
    }

    /**
     * Returns the value of the {@link FieldType#LINE} field.
     *
     * @return the value of the {@link FieldType#LINE} field
     */
    public String getLine() {
        return this.line;
    }

    /**
     * Returns the value of the {@link FieldType#MESSAGE} field.
     *
     * @return the value of the {@link FieldType#MESSAGE} field
     */
    @Override
    public String getMessage() {
        return this.message;
    }

    /**
     * Returns the value of the {@link FieldType#POSITION} field.
     *
     * @return the value of the {@link FieldType#POSITION} field
     */
    public String getPosition() {
        return this.position;
    }

    /**
     * Returns the value of the {@link FieldType#ROUTINE} field.
     *
     * @return the value of the {@link FieldType#ROUTINE} field
     */
    public String getRoutine() {
        return this.routine;
    }

    /**
     * Returns the value of the {@link FieldType#SCHEMA_NAME} field.
     *
     * @return the value of the {@link FieldType#SCHEMA_NAME} field
     */
    public String getSchemaName() {
        return this.schemaName;
    }

    /**
     * Returns the value of the {@link FieldType#SEVERITY_LOCALIZED} field.
     *
     * @return the value of the {@link FieldType#SEVERITY_LOCALIZED} field
     */
    public String getSeverityLocalized() {
        return this.severityLocalized;
    }

    /**
     * Returns the value of the {@link FieldType#SEVERITY_NON_LOCALIZED} field.
     *
     * @return the value of the {@link FieldType#SEVERITY_NON_LOCALIZED} field
     */
    public String getSeverityNonLocalized() {
        return this.severityNonLocalized;
    }

    /**
     * Returns the value of the {@link FieldType#TABLE_NAME} field.
     *
     * @return the value of the {@link FieldType#TABLE_NAME} field
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Returns the value of the {@link FieldType#WHERE} field.
     *
     * @return the value of the {@link FieldType#WHERE} field
     */
    public String getWhere() {
        return this.where;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.code, this.columnName, this.constraintName, this.dataTypeName, this.detail, this.file, this.hint, this.internalPosition, this.internalQuery, this.line,
            this.message, this.position, this.routine, this.schemaName, this.severityLocalized, this.severityNonLocalized, this.tableName, this.where);
    }

    @Override
    public String toString() {
        return "PostgresqlServerErrorException{" +
            "code='" + this.code + '\'' +
            ", columnName='" + this.columnName + '\'' +
            ", constraintName='" + this.constraintName + '\'' +
            ", dataTypeName='" + this.dataTypeName + '\'' +
            ", detail='" + this.detail + '\'' +
            ", file='" + this.file + '\'' +
            ", hint='" + this.hint + '\'' +
            ", internalPosition='" + this.internalPosition + '\'' +
            ", internalQuery='" + this.internalQuery + '\'' +
            ", line='" + this.line + '\'' +
            ", message='" + this.message + '\'' +
            ", position='" + this.position + '\'' +
            ", routine='" + this.routine + '\'' +
            ", schemaName='" + this.schemaName + '\'' +
            ", severityLocalized='" + this.severityLocalized + '\'' +
            ", severityNonLocalized='" + this.severityNonLocalized + '\'' +
            ", tableName='" + this.tableName + '\'' +
            ", where='" + this.where + '\'' +
            "} " + super.toString();
    }

    static void handleErrorResponse(BackendMessage message, SynchronousSink<BackendMessage> sink) {
        if (message instanceof ErrorResponse) {
            sink.error(PostgresqlServerErrorException.toException((ErrorResponse) message));
        } else {
            sink.next(message);
        }
    }

    private static Map<FieldType, String> convertToMap(List<Field> fields) {
        Objects.requireNonNull(fields, "fields must not be null");

        Map <FieldType, String> fieldMap = new HashMap<FieldType, String>(fields.size());
        for ( Field field : fields ){
            fieldMap.put( field.getType(), field.getValue() );
        }
        return fieldMap;
    }

    private static PostgresqlServerErrorException toException(ErrorResponse errorResponse) {
        Objects.requireNonNull(errorResponse, "errorResponse must not be null");

        return new PostgresqlServerErrorException(errorResponse.getFields());
    }

}
