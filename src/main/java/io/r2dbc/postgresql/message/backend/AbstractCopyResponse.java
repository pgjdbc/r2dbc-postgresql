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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

abstract class AbstractCopyResponse implements BackendMessage {

    private final Collection<Format> columnFormats;

    private final Format overallFormat;

    AbstractCopyResponse(Collection<Format> columnFormats, Format overallFormat) {
        this.columnFormats = Assert.requireNonNull(columnFormats, "columnFormats must not be null");
        this.overallFormat = Assert.requireNonNull(overallFormat, "overallFormat must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractCopyResponse that = (AbstractCopyResponse) o;
        return Objects.equals(this.columnFormats, that.columnFormats) &&
            this.overallFormat == that.overallFormat;
    }

    /**
     * Returns the column formats.
     *
     * @return the column formats
     */
    public final Collection<Format> getColumnFormats() {
        return this.columnFormats;
    }

    /**
     * Returns the overall format.
     *
     * @return the overall format
     */
    public final Format getOverallFormat() {
        return this.overallFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columnFormats, this.overallFormat);
    }

    @Override
    public String toString() {
        return "AbstractCopyResponse{" +
            "columnFormats=" + this.columnFormats +
            ", overallFormat=" + this.overallFormat +
            '}';
    }

    static Set<Format> readColumnFormats(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int count = in.readShort();
        Set<Format> formatSet = EnumSet.noneOf(Format.class);

        for (int i = 0; i < count; i++) {
            formatSet.add(Format.valueOf(in.readShort()));
        }

        return formatSet;
    }

}
