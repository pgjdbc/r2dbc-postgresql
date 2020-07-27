package io.r2dbc.postgresql.codec;

import java.util.Objects;

/**
 * Value object that maps to the {@code line} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public final class Line {

    private final double a;
    private final double b;
    private final double c;

    private Line(double a, double b, double c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public static Line of(double a, double b, double c) {
        return new Line(a, b, c);
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
        return "{" + this.a + ","+ this.b + "," + this.c + '}';
    }

}
