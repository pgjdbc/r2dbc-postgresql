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
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Close message.
 */
public final class Close implements FrontendMessage {

    /**
     * The unnamed statement or portal.
     */
    public static final String UNNAMED = "";

    private final String name;

    private final ExecutionType type;

    /**
     * Creates a new message.
     *
     * @param name the name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal)
     * @param type the type to close
     * @throws IllegalArgumentException if {@code name} or {@code type} is {@code null}
     * @see #UNNAMED
     */
    public Close(String name, ExecutionType type) {
        this.name = Assert.requireNonNull(name, "name must not be null");
        this.type = Assert.requireNonNull(type, "type must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();

            writeByte(out, 'C');
            writeLengthPlaceholder(out);
            writeByte(out, this.type.getDiscriminator());
            writeCStringUTF8(out, this.name);

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
        Close that = (Close) o;
        return Objects.equals(this.name, that.name) &&
            this.type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.type);
    }

    @Override
    public String toString() {
        return "Close{" +
            "name='" + this.name + '\'' +
            ", type=" + this.type +
            '}';
    }

}
