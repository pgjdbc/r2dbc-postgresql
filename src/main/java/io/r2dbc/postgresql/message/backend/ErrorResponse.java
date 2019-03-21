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

import java.util.List;
import java.util.Objects;

/**
 * The ErrorResponse message.
 */
public final class ErrorResponse implements BackendMessage {

    private final List<Field> fields;

    /**
     * Creates a new message.
     *
     * @param fields the fields
     * @throws IllegalArgumentException if {@code fields} is {@code null}
     */
    public ErrorResponse(List<Field> fields) {
        this.fields = Assert.requireNonNull(fields, "fields must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ErrorResponse that = (ErrorResponse) o;
        return Objects.equals(this.fields, that.fields);
    }

    /**
     * Returns the fields.
     *
     * @return the fields
     */
    public List<Field> getFields() {
        return this.fields;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fields);
    }

    @Override
    public String toString() {
        return "ErrorResponse{" +
            "fields=" + this.fields +
            '}';
    }

    static ErrorResponse decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        return new ErrorResponse(Field.decode(in));
    }

}
