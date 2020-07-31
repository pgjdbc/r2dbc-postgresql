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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeInt;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Execute message.
 */
public final class Execute implements FrontendMessage, FrontendMessage.DirectEncoder {

    /**
     * No limit on returned rows.
     */
    public static final int NO_LIMIT = 0;

    /**
     * The unnamed portal.
     */
    public static final String UNNAMED_PORTAL = "";

    private final String name;

    private final int rows;

    /**
     * Create a new message.
     *
     * @param name the name of the portal to execute (an empty string selects the unnamed portal)
     * @param rows maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
     * @throws IllegalArgumentException if {@code name} is {@code null}
     * @see #UNNAMED_PORTAL
     * @see #NO_LIMIT
     */
    public Execute(String name, int rows) {
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.rows = rows;
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();
            encode(out);
            return out;
        });
    }

    @Override
    public void encode(ByteBuf byteBuf) {

        writeByte(byteBuf, 'E');

        int writerIndex = byteBuf.writerIndex();

        writeLengthPlaceholder(byteBuf);
        writeCStringUTF8(byteBuf, this.name);
        writeInt(byteBuf, this.rows);
        writeSize(byteBuf, writerIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Execute that = (Execute) o;
        return this.rows == that.rows &&
            Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, rows);
    }

    @Override
    public String toString() {
        return "Execute{" +
            "name='" + this.name + '\'' +
            ", rows=" + this.rows +
            '}';
    }

}
