package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

/**
 * Value object that maps to the {@code line} datatype in Postgres.
 * Lines are represented by the linear equation Ax + By + C = 0.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public final class Line {

    /**
     * Coefficient of x.
     */
    private final double a;

    /**
     * Coefficient of y.
     */
    private final double b;

    /**
     * Constant.
     */
    private final double c;

    private Line(double a, double b, double c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    /**
     * Creates a new {@link Line} given parameters {@code a}, {@code b} and {@code c} of the linear equation.
     *
     * @param a coefficient of x
     * @param b coefficient of y
     * @param c constant
     * @return the new {@link Line} object
     */
    public static Line of(double a, double b, double c) {
        return new Line(a, b, c);
    }

    /**
     * Creates a new {@link Line} defined by two points.
     *
     * @param p1 first point on the line
     * @param p2 second point on the line
     * @return the new {@link Line} object
     * @throws IllegalArgumentException if {@code p1} or {@code p2} is {@code null}
     */
    public static Line of(Point p1, Point p2) {
        Assert.requireNonNull(p1, "p1 must not be null");
        Assert.requireNonNull(p2, "p2 must not be null");

        return of(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    /**
     * Creates a new {@link Line} defined by two points.
     *
     * @param x1 the x-coordinate of the first point on the line
     * @param y1 the y-coordinate of the first point on the line
     * @param x2 the x-coordinate of the second point on the line
     * @param y2 the y-coordinate of the second point on the line
     * @return the new {@link Line} object
     */
    public static Line of(double x1, double y1, double x2, double y2) {

        double a, b, c;

        if (x1 == x2) {
            a = -1;
            b = 0;
        } else {
            a = (y2 - y1) / (x2 - x1);
            b = -1;
        }
        c = y1 - a * x1;

        return of(a, b, c);
    }

    public double getA() {
        return this.a;
    }

    public double getB() {
        return this.b;
    }

    public double getC() {
        return this.c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Line line = (Line) o;
        return Double.compare(line.a, this.a) == 0 &&
            Double.compare(line.b, this.b) == 0 &&
            Double.compare(line.c, this.c) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.a, this.b, this.c);
    }

    @Override
    public String toString() {
        return String.format("{%s,%s,%s}", this.a, this.b, this.c);
    }

}
