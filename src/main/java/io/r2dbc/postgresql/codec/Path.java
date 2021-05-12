package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Value object that maps to the {@code path} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 *
 * @since 0.8.5
 */
public final class Path {

    private final boolean open;

    private final List<Point> points;

    private Path(boolean open, List<Point> points) {
        Assert.requireNonNull(points, "points must not be null");
        this.points = Collections.unmodifiableList(new ArrayList<>(points));
        this.open = open;
    }

    /**
     * Create a new open {@link Path} given {@link List list of points}.
     *
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     */
    public static Path open(List<Point> points) {
        return of(true, points);
    }

    /**
     * Create a new closed {@link Polygon} given {@link List list of points}.
     *
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     */
    public static Path closed(List<Point> points) {
        return of(false, points);
    }

    /**
     * Create a new {@link Polygon} given {@link List list of points}.
     *
     * @param open   whether the path is open or closed
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     * @see #open(List)
     * @see #closed(List)
     */
    public static Path of(boolean open, List<Point> points) {
        return new Path(open, points);
    }

    /**
     * Create a new open {@link Path} given {@code points}.
     *
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     */
    public static Path open(Point... points) {
        return of(true, points);
    }

    /**
     * Create a new closed {@link Polygon} given {@code points}.
     *
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     */
    public static Path closed(Point... points) {
        return of(false, points);
    }

    /**
     * Create a new {@link Polygon} given {@code points}.
     *
     * @param open   whether the path is open or closed
     * @param points the points
     * @return the new {@link Polygon} object
     * @throws IllegalArgumentException if {@code points} is {@code null}
     */
    public static Path of(boolean open, Point... points) {
        Assert.requireNonNull(points, "points must not be null");

        return new Path(open, Arrays.asList(points));
    }

    public boolean isOpen() {
        return this.open;
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
        Path path = (Path) o;
        return this.open == path.open && this.points.equals(path.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.open, this.points);
    }

    @Override
    public String toString() {
        String points = this.points.stream().map(Point::toString).collect(Collectors.joining(", "));
        return isOpen() ? "[" + points + "]" : "(" + points + ")";
    }

}
