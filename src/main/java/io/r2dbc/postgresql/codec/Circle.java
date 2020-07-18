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

public final class Circle {
    private final Point center;
    private final double radius;

    public Circle(Point center, double radius) {
        this.center = center;
        this.radius = radius;
    }

    public Circle(double x, double y, double r) {
        this(Point.of(x, y), r);
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
            return circle.center.equals(center) && circle.radius == radius;
        }
        return false;
    }

    @Override
    public int hashCode() {
        long v = Double.doubleToLongBits(radius);
        return (int) (center.hashCode() ^ v ^ (v >>> 32));
    }

    @Override
    public String toString() {
        return "<" + center.toString() + "," + radius + ">";
    }

}
