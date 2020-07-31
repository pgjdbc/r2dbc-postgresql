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

import java.util.Objects;

/**
 * The CopyData message.
 */
public final class CopyData extends AbstractReferenceCounted implements BackendMessage {

    private final ByteBuf data;

    /**
     * Create a new message.
     *
     * @param data data that forms part of a {@code COPY} data stream.  Always corresponds to a single data row.
     * @throws IllegalArgumentException if {@code data} is {@code null}
     */
    public CopyData(ByteBuf data) {
        Assert.requireNonNull(data, "data must not be null");

        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CopyData copyData = (CopyData) o;
        return Objects.equals(this.data, copyData.data);
    }

    /**
     * Returns data that forms part of a {@code COPY} data stream.  Always corresponds to a single data row.
     *
     * @return data that forms part of a {@code COPY} data stream.
     */
    public ByteBuf getData() {
        return this.data;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data);
    }

    @Override
    public String toString() {
        return "CopyData{" +
            "data=" + this.data +
            '}';
    }

    @Override
    protected void deallocate() {
        this.data.release();
    }

    @Override
    public CopyData touch(Object hint) {
        return this;
    }

    static CopyData decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        return new CopyData(in.readSlice(in.readableBytes()).retain());
    }

}
