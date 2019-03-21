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
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.util.List;

/**
 * The CopyInResponse message.
 */
public final class CopyInResponse extends AbstractCopyResponse {

    /**
     * Creates a new message.
     *
     * @param columnFormats the column formats
     * @param overallFormat the overall format
     */
    public CopyInResponse(List<Format> columnFormats, Format overallFormat) {
        super(columnFormats, overallFormat);
    }

    @Override
    public String toString() {
        return "CopyInResponse{} " + super.toString();
    }

    static CopyInResponse decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        Format overallFormat = Format.valueOf(in.readByte());
        List<Format> columnFormats = readColumnFormats(in);

        return new CopyInResponse(columnFormats, overallFormat);
    }

}
