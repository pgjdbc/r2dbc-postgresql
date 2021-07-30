package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

/**
 * Value object that maps to the {@code box} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 *
 * @since 0.8.5
 */
public final class Box {

    /**
     * Typically upper right corner.
     */
    private final Point a;

    /**
     * Typically lower left corner.
     */
    private final Point b;

    private Box(Point a, Point b) {
        this.a = Assert.requireNonNull(a, "point A must not be null");
        this.b = Assert.requireNonNull(b, "point B must not be null");
    }

    /**
     * Create a new {@link Box} given parameters {@link Point points }{@code a} and {@code b}.
     * <p>Any two opposite corners can be supplied on input, but the values will be reordered as needed to store the upper right and lower left corners, in that order.
     *
     * @param a first corner
     * @param b second corner
     * @return the new {@link Box} object
     * @throws IllegalArgumentException if {@code a} or {@code b} is {@code null}
     */
    public static Box of(Point a, Point b) {
        return new Box(a, b);
    }

    /**
     * Create a new {@link Box} given parameters {@code ax}, {@code ay}, {@code bx} and {@code by}.
     * <p>Any two opposite corners can be supplied on input, but the values will be reordered as needed to store the upper right and lower left corners, in that order.
     *
     * @param ax the x axis coordinate of {@link Point point} a
     * @param ay the y axis coordinate of {@link Point point} a
     * @param bx the x axis coordinate of {@link Point point} b
     * @param by the y axis coordinate of {@link Point point} b
     * @return the new {@link Box} object
     * @since 0.9
     */
    public static Box of(double ax, double ay, double bx, double by) {
        return new Box(Point.of(ax, ay), Point.of(bx, by));
    }

    public Point getA() {
        return this.a;
    }

    public Point getB() {
        return this.b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Box box = (Box) o;
        return (this.a.equals(box.a) && this.b.equals(box.b))
            || (this.a.equals(box.b) && this.b.equals(box.a));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.a, this.b);
    }

    @Override
    public String toString() {
        return "(" + this.a + "," + this.b + ")";
    }

}
