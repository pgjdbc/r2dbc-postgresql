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

import java.util.Arrays;
import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeInt;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeShort;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Parse message.
 */
public final class Parse implements FrontendMessage, FrontendMessage.DirectEncoder {

    /**
     * The unnamed statement.
     */
    public static final String UNNAMED_STATEMENT = "";

    /**
     * The unspecified data type.
     */
    public static final int UNSPECIFIED = 0;

    private final String name;

    private final int[] parameters;

    private final String query;

    /**
     * Create a new message.
     *
     * @param name       the name of the destination prepared statement (an empty string selects the unnamed prepared statement)
     * @param parameters the object IDs of the parameter data types. Placing a zero here is equivalent to leaving the type unspecified.
     * @param query      the query string to be parsed
     * @throws IllegalArgumentException if {@code name}, {@code parameters}, or {@code query} is {@code null}
     * @see #UNNAMED_STATEMENT
     * @see #UNSPECIFIED
     */
    public Parse(String name, int[] parameters, String query) {
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.parameters = Assert.requireNonNull(parameters, "parameters must not be null");
        this.query = Assert.requireNonNull(query, "query must not be null");
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

        writeByte(byteBuf, 'P');
        writeLengthPlaceholder(byteBuf);
        writeCStringUTF8(byteBuf, this.name);
        writeCStringUTF8(byteBuf, this.query);

        writeShort(byteBuf, this.parameters.length);
        for (int parameter : this.parameters) {
            writeInt(byteBuf, parameter);
        }

        writeSize(byteBuf);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Parse that = (Parse) o;
        return Objects.equals(this.name, that.name) &&
            Arrays.equals(this.parameters, that.parameters) &&
            Objects.equals(this.query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.parameters, this.query);
    }

    @Override
    public String toString() {
        return "Parse{" +
            "name='" + this.name + '\'' +
            ", parameters=" + Arrays.toString(this.parameters) +
            ", query='" + this.query + '\'' +
            '}';
    }

}
