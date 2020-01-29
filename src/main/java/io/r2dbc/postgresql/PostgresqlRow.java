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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Row;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link Row} for a PostgreSQL database.
 */
final class PostgresqlRow implements io.r2dbc.postgresql.api.PostgresqlRow {

    private final Codecs codecs;

    private final List<RowDescription.Field> fields;

    private final ByteBuf[] data;

    private volatile boolean isReleased = false;

    PostgresqlRow(Codecs codecs, List<RowDescription.Field> fields, ByteBuf[] data) {
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.fields = Assert.requireNonNull(fields, "fields must not be null");
        this.data = Assert.requireNonNull(data, "data must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PostgresqlRow that = (PostgresqlRow) o;
        return Objects.equals(this.fields, that.fields);
    }

    @Nullable
    @Override
    public <T> T get(int index, Class<T> type) {
        Assert.requireNonNull(type, "type must not be null");
        requireNotReleased();

        return decode(getColumn(index), type);
    }

    @Nullable
    @Override
    public <T> T get(String name, Class<T> type) {
        Assert.requireNonNull(name, "name must not be null");
        Assert.requireNonNull(type, "type must not be null");
        requireNotReleased();

        return decode(getColumn(name), type);
    }

    @Nullable
    private <T> T decode(int index, Class<T> type) {
        ByteBuf data = this.data[index];
        if (data == null) {
            return null;
        }

        int readerIndex = data.readerIndex();
        try {
            RowDescription.Field field = this.fields.get(index);
            return this.codecs.decode(data, field.getDataType(), field.getFormat(), type);
        } finally {
            data.readerIndex(readerIndex);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fields);
    }

    @Override
    public String toString() {
        return "PostgresqlRow{" +
            "codecs=" + this.codecs +
            ", columns=" + this.fields +
            ", isReleased=" + this.isReleased +
            '}';
    }

    static PostgresqlRow toRow(Codecs codecs, DataRow dataRow, RowDescription rowDescription) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(dataRow, "dataRow must not be null");
        Assert.requireNonNull(rowDescription, "rowDescription must not be null");

        return new PostgresqlRow(codecs, rowDescription.getFields(), dataRow.getColumns());
    }

    void release() {

        for (ByteBuf datum : this.data) {
            if (datum != null) {
                datum.release();
            }
        }
        this.isReleased = true;
    }

    private int getColumn(String name) {
        for (int i = 0; i < this.fields.size(); i++) {
            RowDescription.Field field = this.fields.get(i);

            if (field.getName().equalsIgnoreCase(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", name, toColumnNames()));
    }

    private List<String> toColumnNames() {
        List<String> names = new ArrayList<>(this.fields.size());

        for (RowDescription.Field field : this.fields) {
            names.add(field.getName());
        }

        return names;
    }

    private int getColumn(int index) {
        if (index >= this.fields.size()) {
            throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.fields.size()));
        }

        return index;
    }

    private void requireNotReleased() {
        if (this.isReleased) {
            throw new IllegalStateException("Value cannot be retrieved after row has been released");
        }
    }

}
