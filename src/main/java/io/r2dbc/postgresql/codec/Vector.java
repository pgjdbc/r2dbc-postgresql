/*
 * Copyright 2023 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.Arrays;
import java.util.Collection;

/**
 * Value object that maps to the {@code vector} datatype provided by Postgres pgvector.
 *
 * @since 1.0.3
 */
public class Vector {

    private static final Vector EMPTY = new Vector(new float[0]);

    private final float[] vec;

    private Vector(float[] vec) {
        this.vec = Assert.requireNonNull(vec, "Vector must not be null");
    }

    /**
     * Create a new empty {@link Vector}.
     *
     * @return the empty {@link Vector} object
     */
    public static Vector empty() {
        return EMPTY;
    }

    /**
     * Create a new {@link Vector} given {@code vector} points.
     *
     * @param vec the vector values
     * @return the new {@link Vector} object
     */
    public static Vector of(float... vec) {
        Assert.requireNonNull(vec, "Vector must not be null");
        return vec.length == 0 ? empty() : new Vector(vec);
    }

    /**
     * Create a new {@link Vector} given {@code vector} points.
     *
     * @param vec the vector values
     * @return the new {@link Vector} object
     */
    public static Vector of(Collection<? extends Number> vec) {
        Assert.requireNonNull(vec, "Vector must not be null");

        if (vec.isEmpty()) {
            return empty();
        }

        float[] floats = new float[vec.size()];
        int index = 0;
        for (Number number : vec) {
            Number next = Assert.requireNonNull(number, "Vector must not contain null elements");
            floats[index++] = next.floatValue();
        }

        return new Vector(floats);
    }

    /**
     * Return the vector values.
     *
     * @return the vector values.
     */
    public float[] getVector() {
        if (this.vec.length == 0) {
            return this.vec;
        }
        float[] copy = new float[this.vec.length];
        System.arraycopy(this.vec, 0, copy, 0, this.vec.length);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Vector other = (Vector) o;
        return Arrays.equals(this.vec, other.vec);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.vec);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');

        for (int i = 0; i < this.vec.length; i++) {
            if (i != 0) {
                builder.append(',');
            }
            builder.append(this.vec[i]);
        }
        builder.append(']');

        return builder.toString();
    }
}
