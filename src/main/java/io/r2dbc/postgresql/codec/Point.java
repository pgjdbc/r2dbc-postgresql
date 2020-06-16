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

/**
 * Value object that maps to the {@code point} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public final class Point {

    private final double x;

    private final double y;

    private Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Create a new {@link Point} given {@code x} and {@code y} coordinates.
     *
     * @param x the x axis coordinate
     * @param y the y axis coordinate
     * @return the new {@link Point} object
     */
    public static Point of(double x, double y) {
        return new Point(x, y);
    }

    public double getX() {
        return this.x;
    }

    public double getY() {
        return this.y;
    }

    /**
     * Translate the point by the supplied amount by adding {@code x} and {@code y} offsets.
     *
     * @param x integer amount to add on the x axis
     * @param y integer amount to add on the y axis
     * @return new {@link Point} with translated values
     */
    public Point translate(int x, int y) {
        return translate((double) x, (double) y);
    }

    /**
     * Translate the point by the supplied amount by adding {@code x} and {@code y} offsets.
     *
     * @param x double amount to add on the x axis
     * @param y double amount to add on the y axis
     * @return new {@link Point} with translated values
     */
    public Point translate(double x, double y) {
        return new Point(this.x + x, this.y + y);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Point) {
            Point p = (Point) obj;
            return this.x == p.x && this.y == p.y;
        }
        return false;
    }

    @Override
    public int hashCode() {
        long v1 = Double.doubleToLongBits(this.x);
        long v2 = Double.doubleToLongBits(this.y);
        return (int) (v1 ^ v2 ^ (v1 >>> 32) ^ (v2 >>> 32));
    }

    @Override
    public String toString() {
        return "(" + this.x + "," + this.y + ")";
    }

}
