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

package io.r2dbc.postgresql.client;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A collection of {@link EncodedParameter}s for a single bind invocation of an {@link ExtendedQueryMessageFlow}.
 */
public final class Binding {

    public static final Binding EMPTY = new Binding(0);

    private static final EncodedParameter UNSPECIFIED = new EncodedParameter(Format.FORMAT_BINARY, -1, Mono.never());

    private final int expectedSize;

    private final List<EncodedParameter> parameters;

    private final int[] types;

    /**
     * Create a new instance.
     *
     * @param expectedSize the expected number of parameters
     */
    public Binding(int expectedSize) {
        this.expectedSize = expectedSize;
        this.parameters = new ArrayList<>(Collections.nCopies(expectedSize, UNSPECIFIED));
        this.types = new int[expectedSize];
    }

    /**
     * Add a {@link EncodedParameter} to the binding.
     *
     * @param index     the index of the {@link EncodedParameter}
     * @param parameter the {@link EncodedParameter}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(int index, EncodedParameter parameter) {
        Assert.requireNonNull(parameter, "parameter must not be null");

        if (index >= this.expectedSize) {
            throw new IndexOutOfBoundsException(String.format("Binding index %d when only %d parameters are expected", index, this.expectedSize));
        }

        this.parameters.set(index, parameter);
        this.types[index] = parameter.getType();

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
        return getTransformedParameters(EncodedParameter::getFormat);
    }

    /**
     * Returns the types of the parameters in the binding.
     *
     * @return the types of the parameters in the binding
     */
    public int[] getParameterTypes() {

        for (int i = 0; i < this.parameters.size(); i++) {
            EncodedParameter parameter = this.parameters.get(i);
            if (parameter == UNSPECIFIED) {
                throw new IllegalStateException(String.format("No parameter specified for index %d", i));
            }
        }
        return this.types;
    }

    /**
     * Returns the values of the parameters in the binding.
     *
     * @return the values of the parameters in the binding
     */
    public List<Publisher<? extends ByteBuf>> getParameterValues() {
        return getTransformedParameters(EncodedParameter::getValue);
    }

    Flux<Publisher<? extends ByteBuf>> parameterValues() {
        return Flux.fromIterable(this.parameters).map(EncodedParameter::getValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

    public int size() {
        return this.parameters.size();
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            '}';
    }

    /**
     * Validates that the correct number of parameters have been bound.
     *
     * @throws IllegalStateException if the incorrect number of parameters have been bound
     */
    public void validate() {
        for (EncodedParameter parameter : this.parameters) {
            if (UNSPECIFIED == parameter) {
                throw new IllegalStateException("Bound parameter count does not match parameters in SQL statement");
            }
        }
    }

    private <T> List<T> getTransformedParameters(Function<EncodedParameter, T> transformer) {

        if (this.parameters.isEmpty()) {
            return Collections.emptyList();
        }

        List<T> transformed = null;

        for (int i = 0; i < this.parameters.size(); i++) {
            EncodedParameter parameter = this.parameters.get(i);
            if (parameter == UNSPECIFIED) {
                throw new IllegalStateException(String.format("No parameter specified for index %d", i));
            }

            if (transformed == null) {
                if (this.parameters.size() == 1) {
                    return Collections.singletonList(transformer.apply(parameter));
                }

                transformed = new ArrayList<>(this.parameters.size());
            }

            transformed.add(transformer.apply(parameter));
        }

        return transformed;
    }

}
