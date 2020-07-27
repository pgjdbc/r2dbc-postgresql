package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Value object that maps to the {@code path} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public class Path {

    private final List<Point> points;

    private final boolean isOpen;

    private Path(List<Point> points, boolean isOpen) {
        Assert.requireNonNull(points, "points must not be null");
        this.points = Collections.unmodifiableList(new ArrayList<>(points));
        this.isOpen = isOpen;
    }

    public static Path of(boolean isOpen, List<Point> points) {
        return new Path(points, isOpen);
    }

    public static Path of(boolean isOpen, Point... points) {
        return new Path(Arrays.asList(points), isOpen);
    }

    public List<Point> getPoints() {
        return this.points;
    }

    public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Path path = (Path) o;
        return this.points.equals(path.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.points);
    }

}
