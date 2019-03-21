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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link RowMetadata} for a PostgreSQL database.
 */
public final class PostgresqlRowMetadata implements RowMetadata {

    private final List<PostgresqlColumnMetadata> columnMetadatas;

    private final Map<String, PostgresqlColumnMetadata> nameKeyedColumnMetadatas;

    PostgresqlRowMetadata(List<PostgresqlColumnMetadata> columnMetadatas) {
        this.columnMetadatas = Assert.requireNonNull(columnMetadatas, "columnMetadatas must not be null");

        this.nameKeyedColumnMetadatas = getNameKeyedColumnMetadatas(columnMetadatas);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PostgresqlRowMetadata that = (PostgresqlRowMetadata) o;
        return Objects.equals(this.columnMetadatas, that.columnMetadatas);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code identifier} does not correspond to a column
     */
    @Override
    public ColumnMetadata getColumnMetadata(Object identifier) {
        Assert.requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer) {
            return getColumnMetadata((Integer) identifier);
        } else if (identifier instanceof String) {
            return getColumnMetadata((String) identifier);
        }

        throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
    }

    @Override
    public List<PostgresqlColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(this.columnMetadatas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columnMetadatas);
    }

    @Override
    public String toString() {
        return "PostgresqlRowMetadata{" +
            "columnMetadatas=" + this.columnMetadatas +
            ", nameKeyedColumnMetadatas=" + this.nameKeyedColumnMetadatas +
            '}';
    }

    static PostgresqlRowMetadata toRowMetadata(Codecs codecs, RowDescription rowDescription) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(rowDescription, "rowDescription must not be null");

        return new PostgresqlRowMetadata(getColumnMetadatas(codecs, rowDescription));
    }

    private static List<PostgresqlColumnMetadata> getColumnMetadatas(Codecs codecs, RowDescription rowDescription) {
        List<PostgresqlColumnMetadata> columnMetadatas = new ArrayList<>(rowDescription.getFields().size());

        for (RowDescription.Field field : rowDescription.getFields()) {
            columnMetadatas.add(PostgresqlColumnMetadata.toColumnMetadata(codecs, field));
        }

        return columnMetadatas;
    }

    private ColumnMetadata getColumnMetadata(Integer index) {
        if (index >= this.columnMetadatas.size()) {
            throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.columnMetadatas.size()));
        }

        return this.columnMetadatas.get(index);
    }

    private ColumnMetadata getColumnMetadata(String name) {
        if (!this.nameKeyedColumnMetadatas.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", name, this.nameKeyedColumnMetadatas.keySet()));
        }

        return this.nameKeyedColumnMetadatas.get(name);
    }

    private Map<String, PostgresqlColumnMetadata> getNameKeyedColumnMetadatas(List<PostgresqlColumnMetadata> columnMetadatas) {
        Map<String, PostgresqlColumnMetadata> nameKeyedColumnMetadatas = new HashMap<>(columnMetadatas.size());

        for (PostgresqlColumnMetadata columnMetadata : columnMetadatas) {
            nameKeyedColumnMetadatas.put(columnMetadata.getName(), columnMetadata);
        }

        return nameKeyedColumnMetadatas;
    }

}
