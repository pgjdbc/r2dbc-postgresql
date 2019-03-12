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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.RowDescription.Field;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ColumnMetadata;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * An implementation of {@link ColumnMetadata} for a PostgreSQL database.
 */
public final class PostgresqlColumnMetadata implements ColumnMetadata {

    private final Class<?> javaType;

    private final String name;

    private final Integer nativeType;

    private final Short precision;

    PostgresqlColumnMetadata(@Nullable Class<?> javaType, String name, Integer nativeType, Short precision) {
        this.javaType = javaType;
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.nativeType = Assert.requireNonNull(nativeType, "nativeType must not be null");
        this.precision = Assert.requireNonNull(precision, "precision must not be null");
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
        return Objects.equals(this.javaType, that.javaType) &&
            this.name.equals(that.name) &&
            this.nativeType.equals(that.nativeType) &&
            this.precision.equals(that.precision);
    }

    @Override
    public Class<?> getJavaType() {
        return this.javaType;
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
        return this.precision.intValue();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.javaType, this.name, this.nativeType, this.precision);
    }

    @Override
    public String toString() {
        return "PostgresqlColumnMetadata{" +
            "javaType=" + this.javaType +
            ", name='" + this.name + '\'' +
            ", nativeType=" + this.nativeType +
            ", precision=" + this.precision +
            '}';
    }

    static PostgresqlColumnMetadata toColumnMetadata(Codecs codecs, Field field) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(field, "field must not be null");

        return new PostgresqlColumnMetadata(codecs.preferredType(field.getDataType(), field.getFormat()), field.getName(), field.getDataType(), field.getDataTypeSize());
    }

}
