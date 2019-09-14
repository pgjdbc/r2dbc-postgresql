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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeBytes;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeInt;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeShort;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Bind message.
 */
public final class Bind implements FrontendMessage {

    /**
     * A marker indicating a {@code NULL} value.
     */
    public static final ByteBuf NULL_VALUE = Unpooled.EMPTY_BUFFER;

    /**
     * The unnamed portal.
     */
    public static final String UNNAMED_PORTAL = "";

    /**
     * The unnamed statement.
     */
    public static final String UNNAMED_STATEMENT = "";

    private static final int NULL = -1;

    private final String name;

    private final Collection<Format> parameterFormats;

    private final Collection<ByteBuf> parameters;

    private final Collection<Format> resultFormats;

    private final String source;

    /**
     * Creates a new message.
     *
     * @param name             the name of the destination portal (an empty string selects the unnamed portal)
     * @param parameterFormats the parameter formats
     * @param parameters       the value of the parameters, in the format indicated by the associated format
     * @param resultFormats    the result formats
     * @param source           the name of the source prepared statement (an empty string selects the unnamed prepared statement)
     * @throws IllegalArgumentException if {@code name}, {@code parameterFormats}, {@code parameters}, {@code resultFormats}, or {@code source} is {@code null}
     * @see #UNNAMED_PORTAL
     * @see #UNNAMED_STATEMENT
     */
    public Bind(String name, List<Format> parameterFormats, List<ByteBuf> parameters, Collection<Format> resultFormats, String source) {
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.parameterFormats = Assert.requireNonNull(parameterFormats, "parameterFormats must not be null");
        this.parameters = Assert.requireNonNull(parameters, "parameters must not be null");
        this.resultFormats = Assert.requireNonNull(resultFormats, "resultFormats must not be null");
        this.source = Assert.requireNonNull(source, "source must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();

            writeByte(out, 'B');
            writeLengthPlaceholder(out);
            writeCStringUTF8(out, this.name);
            writeCStringUTF8(out, this.source);

            writeShort(out, this.parameterFormats.size());
            this.parameterFormats.forEach(format -> writeShort(out, format.getDiscriminator()));

            writeShort(out, this.parameters.size());
            this.parameters.forEach(parameters -> {
                if (parameters == NULL_VALUE) {
                    writeInt(out, NULL);
                } else {
                    writeInt(out, parameters.readableBytes());
                    writeBytes(out, parameters);
                    parameters.release();
                }
            });

            writeShort(out, this.resultFormats.size());
            this.resultFormats.forEach(format -> writeShort(out, format.getDiscriminator()));

            return writeSize(out);
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bind that = (Bind) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.parameterFormats, that.parameterFormats) &&
            Objects.equals(this.parameters, that.parameters) &&
            Objects.equals(this.resultFormats, that.resultFormats) &&
            Objects.equals(this.source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.parameterFormats, this.parameters, this.resultFormats, this.source);
    }

    @Override
    public String toString() {
        return "Bind{" +
            "name='" + this.name + '\'' +
            ", parameterFormats=" + this.parameterFormats +
            ", parameters=" + this.parameters +
            ", resultFormats=" + this.resultFormats +
            ", source='" + this.source + '\'' +
            '}';
    }

}
