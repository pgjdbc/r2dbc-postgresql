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

public final class LineSegment {

    private final Point point1;

    private final Point point2;

    public LineSegment(Point point1, Point point2) {
        this.point1 = Assert.requireNonNull(point1, "First Point must not be null");
        this.point2 = Assert.requireNonNull(point2, "Second Point must not be null");
    }

    public LineSegment(double x1, double y1, double x2, double y2) {
        this.point1 = Point.of(x1, y1);
        this.point2 = Point.of(x2, y2);
    }

    public Point getPoint1() {
        return point1;
    }

    public Point getPoint2() {
        return point2;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LineSegment) {
            LineSegment lineSegment = (LineSegment) obj;
            return (lineSegment.point1.equals(this.point1) && lineSegment.point2.equals(this.point2))
                || (lineSegment.point1.equals(this.point2) && lineSegment.point2.equals(this.point1));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return point1.hashCode() ^ point2.hashCode();
    }

    @Override
    public String toString() {
        return "[" + this.point1.toString() + "," + this.point2.toString() + "]";
    }

}
