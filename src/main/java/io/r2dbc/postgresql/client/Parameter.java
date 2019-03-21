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

package io.r2dbc.postgresql.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * A parameter bound to an {@link ExtendedQueryMessageFlow}.
 */
public final class Parameter {

    private final Format format;

    private final Integer type;

    private final ByteBuf value;

    /**
     * Creates a new instance.
     *
     * @param format the {@link Format} of the parameter
     * @param type   the type of the parameter
     * @param value  the value of the parameter
     * @throws IllegalArgumentException if {@code format}, or {@code type} is {@code null}
     */
    public Parameter(Format format, Integer type, @Nullable ByteBuf value) {
        this.format = Assert.requireNonNull(format, "format must not be null");
        this.type = Assert.requireNonNull(type, "type must not be null");
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Parameter that = (Parameter) o;
        return this.format == that.format &&
            Objects.equals(this.type, that.type) &&
            Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.format, this.type, this.value);
    }

    @Override
    public String toString() {
        return "Parameter{" +
            "format=" + this.format +
            ", type=" + this.type +
            ", value=" + this.value +
            '}';
    }

    /**
     * Returns the format of the parameter.
     *
     * @return the format of the parameter
     */
    Format getFormat() {
        return this.format;
    }

    /**
     * Returns the type of the parameter.
     *
     * @return the type of the parameter
     */
    Integer getType() {
        return this.type;
    }

    /**
     * Returns the value of the parameter.
     *
     * @return the value of the parameter
     */
    @Nullable
    ByteBuf getValue() {
        return this.value;
    }

}
