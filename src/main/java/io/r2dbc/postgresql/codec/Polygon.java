package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Value object that maps to the {@code polygon} datatype in Postgres.
 */
public final class Polygon {

    private final List<Point> points;

    private Polygon(List<Point> points) {
        Assert.requireNonNull(points, "points must not be null");
        this.points = Collections.unmodifiableList(new ArrayList<>(points));
    }

    public static Polygon of(List<Point> points) {
        return new Polygon(points);
    }

    public static Polygon of(Point... points) {
        return new Polygon(Arrays.asList(points));
    }

    public List<Point> getPoints() {
        return this.points;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Polygon polygon = (Polygon) o;
        return this.points.equals(polygon.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.points);
    }

}
