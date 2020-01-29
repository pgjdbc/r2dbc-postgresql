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
import io.r2dbc.postgresql.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The ParameterDescription message.
 */
public final class ParameterDescription implements BackendMessage {

    private final List<Integer> parameters;

    /**
     * Creates a new message.
     *
     * @param parameters the object IDs of the parameter data types
     * @throws IllegalArgumentException if {@code parameters} is {@code null}
     */
    public ParameterDescription(List<Integer> parameters) {
        this.parameters = Assert.requireNonNull(parameters, "parameters must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParameterDescription that = (ParameterDescription) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    /**
     * Returns the object IDs of the parameter data types.
     *
     * @return the object IDs of the parameter data types
     */
    public List<Integer> getParameters() {
        return this.parameters;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    @Override
    public String toString() {
        return "ParameterDescription{" +
            "parameters=" + this.parameters +
            '}';
    }

    static ParameterDescription decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int count = in.readShort();
        List<Integer> parameters = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            parameters.add(in.readInt());
        }

        return new ParameterDescription(parameters);
    }

}
