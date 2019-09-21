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
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * An implementation of {@link RowMetadata} for a PostgreSQL database.
 */
final class PostgresqlRowMetadata implements io.r2dbc.postgresql.api.PostgresqlRowMetadata {

    private static final Comparator<String> IGNORE_CASE_COMPARATOR = (o1, o2) -> o2.compareToIgnoreCase(o1);

    private final List<PostgresqlColumnMetadata> columnMetadatas;

    private volatile Collection<String> columnNames;

    PostgresqlRowMetadata(List<PostgresqlColumnMetadata> columnMetadatas) {
        this.columnMetadatas = Assert.requireNonNull(columnMetadatas, "columnMetadatas must not be null");
    }

    @Override
    public PostgresqlColumnMetadata getColumnMetadata(int index) {
        if (index >= this.columnMetadatas.size()) {
            throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.columnMetadatas.size()));
        }

        return this.columnMetadatas.get(index);
    }

    @Override
    public PostgresqlColumnMetadata getColumnMetadata(String name) {
        Assert.requireNonNull(name, "name must not be null");

        for (PostgresqlColumnMetadata metadata : this.columnMetadatas) {

            if (metadata.getName().equalsIgnoreCase(name)) {
                return metadata;
            }
        }

        throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", name, getColumnNames()));
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

    @Override
    public List<PostgresqlColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(this.columnMetadatas);
    }

    @Override
    public Collection<String> getColumnNames() {

        if (this.columnNames == null) {
            this.columnNames = getColumnNames(this.columnMetadatas);
        }

        return Collections.unmodifiableCollection(this.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columnMetadatas);
    }

    @Override
    public String toString() {
        return "PostgresqlRowMetadata{" +
            "columnMetadatas=" + this.columnMetadatas +
            ", columnNames=" + this.columnNames +
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

    private Collection<String> getColumnNames(List<PostgresqlColumnMetadata> columnMetadatas) {
        Set<String> columnNames = new TreeSet<>(IGNORE_CASE_COMPARATOR);

        for (PostgresqlColumnMetadata columnMetadata : columnMetadatas) {
            columnNames.add(columnMetadata.getName());
        }

        return columnNames;
    }


}
