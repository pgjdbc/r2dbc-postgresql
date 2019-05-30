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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The DataRow message.
 */
public final class DataRow implements BackendMessage {

    private static final int NULL = -1;

    private final List<ByteBuf> columns;

    /**
     * Creates a new message.
     *
     * @param columns the values of the columns
     * @throws IllegalArgumentException if {@code columns} is {@code null}
     */
    public DataRow(List<ByteBuf> columns) {
        this.columns = Assert.requireNonNull(columns, "columns must not be null");

        for (ByteBuf column : this.columns) {
            if (column != null) {
                column.retain();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataRow dataRow = (DataRow) o;
        return Objects.equals(this.columns, dataRow.columns);
    }

    /**
     * Returns the values of the columns.
     *
     * @return the values of the columns
     */
    public List<ByteBuf> getColumns() {
        return columns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columns);
    }

    /**
     * Release the data encapsulated by the message.
     */
    public void release() {
        for (ByteBuf column : this.columns) {
            if (column != null) {
                column.release();
            }
        }
    }

    @Override
    public String toString() {
        return "DataRow{" +
            "columns=" + this.columns +
            '}';
    }

    static DataRow decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int columnCount = in.readShort();
        List<ByteBuf> columns = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            columns.add(decodeColumn(in));
        }
        return new DataRow(columns);
    }

    @Nullable
    private static ByteBuf decodeColumn(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int length = in.readInt();
        return NULL == length ? null : in.readSlice(length);
    }

}
