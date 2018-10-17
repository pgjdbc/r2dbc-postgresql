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
import io.r2dbc.postgresql.PostgresqlBindingException;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.spi.R2dbcException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;

/**
 * A collection of {@link Parameter}s for a single bind invocation of an {@link ExtendedQueryMessageFlow}.
 */
public final class Binding {

    private static final Parameter UNSPECIFIED_PARAMETER = new Parameter(TEXT, UNSPECIFIED.getObjectId(), null);

    private final List<Parameter> parameters = new ArrayList<>();

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

        // ensure that any skipped parameters are set to null
        int curSize = parameters.size();

        if (curSize < index) {
            while (curSize < index) {
                this.parameters.add(curSize++, null);
            }
            this.parameters.add(index, parameter);
        } else {
            this.parameters.set(index, parameter);
        }
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

    public List<Format> getParameterFormats()  {
        List <Format> formats = new ArrayList<>();
        for (Parameter parameter : parameters ){
            if (parameter == null) {
                throw new PostgresqlBindingException( "Null parameter found");
            }
            formats.add( parameter.getFormat() );
        }
        return formats;
    }

    /**
     * Returns the types of the parameters in the binding.
     *
     * @return the types of the parameters in the binding
     */
    public List<Integer> getParameterTypes()  {
        List <Integer> types = new ArrayList<>();
        for (Parameter parameter : parameters ){
            if (parameter == null) {
                throw new PostgresqlBindingException( "Null parameter found");
            }
            types.add( parameter.getType() );
        }
        return types;
    }

    /**
     * Returns the values of the parameters in the binding.
     *
     * @return the values of the parameters in the binding
     */
    public List<ByteBuf> getParameterValues()  {
        List <ByteBuf> values = new ArrayList<>();
        for (Parameter parameter : parameters ){
            if (parameter == null) {
                throw new PostgresqlBindingException( "Null parameter found");
            }
            values.add( parameter.getValue() );
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

    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

}
