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
import io.r2dbc.postgresql.PostgresqlBindingException;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A collection of {@link Parameter}s for a single bind invocation of an {@link ExtendedQueryMessageFlow}.
 */
public final class Binding {

    private final List<Parameter> parameters = new ArrayList<>();

    /**
     * Add a {@link Parameter} to the binding.
     *
     * @param index     the index of the {@link Parameter}
     * @param parameter the {@link Parameter}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(Integer index, Parameter parameter) {
        Assert.requireNonNull(index, "index must not be null");
        Assert.requireNonNull(parameter, "parameter must not be null");

        if (this.parameters.size() > index) {
            this.parameters.set(index, parameter);
        } else {
            for (int i = this.parameters.size(); i < index; i++) {
                this.parameters.add(null);
            }

            this.parameters.add(parameter);
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

    public List<Format> getParameterFormats() {
        return getTransformedParameters(Parameter::getFormat);
    }

    /**
     * Returns the types of the parameters in the binding.
     *
     * @return the types of the parameters in the binding
     */
    public List<Integer> getParameterTypes() {
        return getTransformedParameters(Parameter::getType);
    }

    /**
     * Returns the values of the parameters in the binding.
     *
     * @return the values of the parameters in the binding
     */
    public List<Publisher<? extends ByteBuf>> getParameterValues() {
        return getTransformedParameters(Parameter::getValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            '}';
    }

    private <T> List<T> getTransformedParameters(Function<Parameter, T> transformer) {
        List<T> transformed = new ArrayList<>(this.parameters.size());

        for (int i = 0; i < this.parameters.size(); i++) {
            Parameter parameter = this.parameters.get(i);
            if (parameter == null) {
                throw new PostgresqlBindingException(String.format("No parameter specified for index %d", i));
            }

            transformed.add(transformer.apply(parameter));
        }

        return transformed;
    }

}
