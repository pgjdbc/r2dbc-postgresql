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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Arrays;

/**
 * The DataRow message.
 */
public final class DataRow extends AbstractReferenceCounted implements BackendMessage {

    private static final int NULL = -1;

    private final ByteBuf[] columns;

    /**
     * Create a new message.
     *
     * @param columns the values of the columns
     * @throws IllegalArgumentException if {@code columns} is {@code null}
     */
    public DataRow(ByteBuf... columns) {
        this.columns = Assert.requireNonNull(columns, "columns must not be null");
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
        return Arrays.equals(this.columns, dataRow.columns);
    }

    /**
     * Returns the values of the columns.
     *
     * @return the values of the columns
     */
    public ByteBuf[] getColumns() {
        return this.columns;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.columns);
    }

    @Override
    public String toString() {
        return "DataRow{" +
            "columns=" + Arrays.toString(this.columns) +
            '}';
    }

    @Override
    protected void deallocate() {

        for (ByteBuf column : this.columns) {
            if (column != null) {
                column.release();
            }
        }
    }

    @Override
    public DataRow touch(Object hint) {
        return this;
    }

    static DataRow decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int columnCount = in.readShort();
        ByteBuf[] columns = new ByteBuf[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columns[i] = decodeColumn(in);
        }
        return new DataRow(columns);
    }

    @Nullable
    private static ByteBuf decodeColumn(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int length = in.readInt();
        return NULL == length ? null : in.readRetainedSlice(length);
    }

}
