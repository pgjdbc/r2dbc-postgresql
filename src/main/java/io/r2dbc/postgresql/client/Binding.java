/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;

/**
 * A collection of {@link Parameter}s for a single bind invocation of an {@link ExtendedQueryMessageFlow}.
 */
public final class Binding {

    private static final Parameter UNSPECIFIED_PARAMETER = new Parameter(TEXT, UNSPECIFIED.getObjectId(), null);

    private final SortedMap<Integer, Parameter> parameters = new TreeMap<>();

    /**
     * Add a {@link Parameter} to the binding.
     *
     * @param index     the index of the {@link Parameter}
     * @param parameter the {@link Parameter}
     * @return this {@link Binding}
     * @throws NullPointerException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(Integer index, Parameter parameter) {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(parameter, "parameter must not be null");

        this.parameters.put(index, parameter);

        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Binding that = (Binding) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    /**
     * Returns the formats of the parameters in the binding.
     *
     * @return the formats of the parameters in the binding
     */
    public List<Format> getParameterFormats() {
        ArrayList<Format> format = new ArrayList<Format>(this.parameters.size());
        for( Parameter p : this.parameters.values()){
            format.add(p.getFormat());
        }
        return format;
    }

    /**
     * Returns the types of the parameters in the binding.
     *
     * @return the types of the parameters in the binding
     */
    public List<Integer> getParameterTypes() {
        ArrayList<Integer> types = new ArrayList<Integer>(this.parameters.size());
        for( Parameter p : this.parameters.values()){
            types.add(p.getType());
        }
        return types;
    }

    /**
     * Returns the values of the parameters in the binding.
     *
     * @return the values of the parameters in the binding
     */
    public List<ByteBuf> getParameterValues() {

        ArrayList<ByteBuf> values = new ArrayList<ByteBuf>(this.parameters.size());
        for( Parameter p : this.parameters.values()){
            values.add(p.getValue());
        }
        return values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            '}';
    }

    private Parameter get(Integer identifier) {
        return this.parameters.getOrDefault(identifier, UNSPECIFIED_PARAMETER);
    }

    private int size() {
        return this.parameters.isEmpty() ? 0 : this.parameters.lastKey() + 1;
    }

}
