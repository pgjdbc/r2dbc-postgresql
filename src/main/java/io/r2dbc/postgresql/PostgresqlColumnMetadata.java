/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.codec.PostgresqlObjectId;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.RowDescription.Field;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Type;

import java.util.Objects;

/**
 * An implementation of {@link ColumnMetadata} for a PostgreSQL database.
 */
final class PostgresqlColumnMetadata implements io.r2dbc.postgresql.api.PostgresqlColumnMetadata {

    private final Codecs codecs;

    private final Format format;

    private final String name;

    private final int nativeType;

    private final short precision;

    PostgresqlColumnMetadata(Codecs codecs, String name, int nativeType, short precision) {
        this.codecs = codecs;
        this.format = Format.FORMAT_TEXT;
        this.name = name;
        this.nativeType = nativeType;
        this.precision = precision;
    }

    PostgresqlColumnMetadata(Codecs codecs, Field field) {
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(field, "field must not be null");
        this.format = field.getFormat();
        this.name = field.getName();
        this.nativeType = field.getDataType();
        this.precision = field.getDataTypeSize();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PostgresqlColumnMetadata)) {
            return false;
        }
        PostgresqlColumnMetadata that = (PostgresqlColumnMetadata) o;
        return
            this.name.equals(that.name) &&
                this.nativeType == that.nativeType &&
                this.precision == that.precision;
    }

    @Override
    public Class<?> getJavaType() {
        return this.codecs.preferredType(this.nativeType, this.format);
    }

    @Override
    public Type getType() {

        if (PostgresqlObjectId.isValid(this.nativeType)) {
            return new OidType(PostgresqlObjectId.valueOf(this.nativeType), getJavaType());
        }

        return new UnknownOidType(this.nativeType, getJavaType());
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Integer getNativeTypeMetadata() {
        return this.nativeType;
    }

    @Override
    public Integer getPrecision() {
        return (int) this.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.nativeType, this.precision);
    }

    @Override
    public String toString() {
        return "PostgresqlColumnMetadata{" +
            ", name='" + this.name + '\'' +
            ", nativeType=" + this.nativeType +
            ", precision=" + this.precision +
            '}';
    }

    static PostgresqlColumnMetadata toColumnMetadata(Codecs codecs, Field field) {
        return new PostgresqlColumnMetadata(codecs, field);
    }

    static class OidType implements Type {

        private final PostgresqlObjectId objectId;

        private final Class<?> javaType;

        OidType(PostgresqlObjectId objectId, Class<?> javaType) {
            this.objectId = objectId;
            this.javaType = javaType;
        }

        @Override
        public Class<?> getJavaType() {
            return this.javaType;
        }

        @Override
        public String getName() {
            return this.objectId.name();
        }

    }

    static class UnknownOidType implements Type {

        private final int oid;

        private final Class<?> javaType;

        UnknownOidType(int oid, Class<?> javaType) {
            this.oid = oid;
            this.javaType = javaType;
        }

        @Override
        public Class<?> getJavaType() {
            return this.javaType;
        }

        @Override
        public String getName() {
            return "" + this.oid;
        }

    }

}
