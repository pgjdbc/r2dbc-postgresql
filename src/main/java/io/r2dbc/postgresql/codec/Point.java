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

package io.r2dbc.postgresql.codec;

/**
 * <p>It maps to the point datatype in org.postgresql.</p>
 *
 * <p> It uses double to represent the coordinates.</p>
 */
public class Point {

    private final double x;

    private final double y;

    /**
     * @param x coordinate
     * @param y coordinate
     */
    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    /**
     * Translate the point by the supplied amount.
     *
     * @param x integer amount to add on the x axis
     * @param y integer amount to add on the y axis
     * @return - new point with translated values
     */
    public Point translate(int x, int y) {
        return translate((double) x, (double) y);
    }

    /**
     * Translate the point by the supplied amount.
     *
     * @param x double amount to add on the x axis
     * @param y double amount to add on the y axis
     * @return - new point with translated values
     */
    public Point translate(double x, double y) {
        return new Point(this.x + x, this.y + y);
    }

    /**
     * Moves the point to the supplied coordinates.
     *
     * @param x integer coordinate
     * @param y integer coordinate
     */
    public Point move(int x, int y) {
        return setLocation(x, y);
    }

    /**
     * Moves the point to the supplied coordinates.
     *
     * @param x double coordinate
     * @param y double coordinate
     * @return - new point with provided coordinates
     */
    public Point move(double x, double y) {
        return new Point(x, y);
    }

    /**
     * Moves the point to the supplied coordinates.
     *
     * @param x integer coordinate
     * @param y integer coordinate
     * @return - return new Point with these coordinates
     */
    public Point setLocation(int x, int y) {
        return move((double) x, (double) y);
    }

    public boolean equals(Object obj) {
        if (obj instanceof Point) {
            Point p = (Point) obj;
            return x == p.x && y == p.y;
        }
        return false;
    }

    public int hashCode() {
        long v1 = Double.doubleToLongBits(x);
        long v2 = Double.doubleToLongBits(y);
        return (int) (v1 ^ v2 ^ (v1 >>> 32) ^ (v2 >>> 32));
    }

    public String toString() {
        return "(" + x + "," + y + ")";
    }

}
