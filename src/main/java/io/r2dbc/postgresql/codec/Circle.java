/*
 * Copyright 2020 the original author or authors.
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

/**
 * Value object that maps to the {@code circle} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 *
 * @since 0.8.5
 */
public final class Circle {

    private final Point center;

    private final double radius;

    private Circle(Point center, double radius) {
        this.center = Assert.requireNonNull(center, "center must not be null");
        this.radius = radius;
    }

    /**
     * Create a new {@link Circle} given {@link Point center} and {@code radius}.
     *
     * @param center the center point
     * @param radius the radius
     * @return the new {@link Circle} object
     * @throws IllegalArgumentException if {@code radius} is {@code null}
     */
    public static Circle of(Point center, double radius) {
        return new Circle(center, radius);
    }

    /**
     * Create a new {@link Circle} given center coordinates {@code x/y} and {@code radius}.
     *
     * @param x      the x center coordinate
     * @param y      the y center coordinate
     * @param radius the radius
     * @return the new {@link Circle} object
     */
    public static Circle of(double x, double y, double radius) {
        return new Circle(Point.of(x, y), radius);
    }

    public Point getCenter() {
        return this.center;
    }

    public double getRadius() {
        return this.radius;
    }

    /**
     * @param obj Object to compare with
     * @return true if the two circles are identical
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Circle) {
            Circle circle = (Circle) obj;
            return circle.center.equals(this.center) && circle.radius == this.radius;
        }
        return false;
    }

    @Override
    public int hashCode() {
        long v = Double.doubleToLongBits(this.radius);
        return (int) (this.center.hashCode() ^ v ^ (v >>> 32));
    }

    @Override
    public String toString() {
        return String.format("<%s,%s>", this.center.toString(), this.radius);
    }

}
