/*
 * Copyright 2017 the original author or authors.
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
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.RefCursor;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Row;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An implementation of {@link Row} for a PostgreSQL database.
 */
final class PostgresqlRow implements io.r2dbc.postgresql.api.PostgresqlRow {

    private final ConnectionResources context;

    private final io.r2dbc.postgresql.api.PostgresqlRowMetadata metadata;

    private final List<RowDescription.Field> fields;

    private final ByteBuf[] data;

    private volatile boolean isReleased = false;

    private Map<String, Integer> columnNameIndexCacheMap;

    PostgresqlRow(ConnectionResources context, io.r2dbc.postgresql.api.PostgresqlRowMetadata metadata, List<RowDescription.Field> fields, ByteBuf[] data) {
        this.context = Assert.requireNonNull(context, "context must not be null");
        this.metadata = Assert.requireNonNull(metadata, "metadata must not be null");
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

    @Override
    public io.r2dbc.postgresql.api.PostgresqlRowMetadata getMetadata() {
        return this.metadata;
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

            T decoded = this.context.getCodecs().decode(data, field.getDataType(), field.getFormat(), type);

            return type.cast(postProcessResult(decoded));

        } finally {
            data.readerIndex(readerIndex);
        }
    }

    @Nullable
    private Object postProcessResult(@Nullable Object decoded) {

        if (decoded instanceof RefCursor) {
            return createCursor((RefCursor) decoded);
        }

        return decoded;
    }

    private AttachedRefCursor createCursor(RefCursor decoded) {
        return new AttachedRefCursor(this.context, decoded.getCursorName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fields);
    }

    @Override
    public String toString() {
        return "PostgresqlRow{" +
            "context=" + this.context +
            ", columns=" + this.fields +
            ", isReleased=" + this.isReleased +
            '}';
    }

    static Map<String, Integer> createColumnNameIndexMap(List<RowDescription.Field> fields) {
        Map<String, Integer> columnNameIndexMap = new HashMap<>(fields.size() * 2);
        for (int i = fields.size() - 1; i >= 0; i--) {
            columnNameIndexMap.put(fields.get(i).getName().toLowerCase(Locale.US), i);
        }

        return columnNameIndexMap;
    }

    static PostgresqlRow toRow(ConnectionResources context, DataRow dataRow, Codecs codecs, RowDescription rowDescription) {
        Assert.requireNonNull(dataRow, "dataRow must not be null");
        Assert.requireNonNull(codecs, "rowDescription must not be null");
        Assert.requireNonNull(rowDescription, "rowDescription must not be null");

        return new PostgresqlRow(context, PostgresqlRowMetadata.toRowMetadata(codecs, rowDescription), rowDescription.getFields(), dataRow.getColumns());
    }

    static PostgresqlRow toRow(ConnectionResources context, DataRow dataRow, PostgresqlRowMetadata metadata, RowDescription rowDescription) {
        Assert.requireNonNull(dataRow, "dataRow must not be null");
        Assert.requireNonNull(metadata, "metadata must not be null");
        Assert.requireNonNull(rowDescription, "rowDescription must not be null");

        return new PostgresqlRow(context, metadata, rowDescription.getFields(), dataRow.getColumns());
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
        if (this.columnNameIndexCacheMap == null) {
            this.columnNameIndexCacheMap = createColumnNameIndexMap(this.fields);
        }

        Integer index = this.columnNameIndexCacheMap.get(name);
        if (index != null) {
            return index;
        }

        index = this.columnNameIndexCacheMap.get(name.toLowerCase(Locale.US));
        if (index != null) {
            this.columnNameIndexCacheMap.put(name, index);
            return index;
        }

        throw new NoSuchElementException(String.format("Column name '%s' does not exist in column names %s", name, toColumnNames()));
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
            throw new IndexOutOfBoundsException(String.format("Column index %d is larger than the number of columns %d", index, this.fields.size()));
        }

        return index;
    }

    private void requireNotReleased() {
        if (this.isReleased) {
            throw new IllegalStateException("Value cannot be retrieved after row has been released");
        }
    }

    /**
     * Default {@link RefCursor} implementation that is attached to a {@link io.r2dbc.postgresql.api.PostgresqlConnection}.
     */
    static class AttachedRefCursor implements RefCursor {

        private final ConnectionResources context;

        private final String portal;

        AttachedRefCursor(ConnectionResources context, String portal) {
            this.context = Assert.requireNonNull(context, "connection must not be null");
            this.portal = Assert.requireNotEmpty(portal, "portal must not be empty");
        }

        @Override
        public String getCursorName() {
            return this.portal;
        }

        @Override
        public Mono<io.r2dbc.postgresql.api.PostgresqlResult> fetch() {
            return Mono.fromDirect(this.context.getConnection().createStatement("FETCH ALL IN \"" + getCursorName() + "\"").execute());
        }

        @Override
        public Mono<Void> close() {
            return this.context.getConnection().createStatement("CLOSE \"" + getCursorName() + "\"").execute().flatMap(PostgresqlResult::getRowsUpdated).then();
        }

        @Override
        public String toString() {
            return "AttachedRefCursor{" +
                "portal='" + this.portal + '\'' +
                ", context=" + this.context +
                '}';
        }

    }

}
