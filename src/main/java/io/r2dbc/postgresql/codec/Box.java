package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

/**
 * Value object that maps to the {@code box} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public final class Box {

    private final Point a;

    private final Point b;

    private Box(Point a, Point b) {
        this.a = Assert.requireNonNull(a, "point A must not be null");
        this.b = Assert.requireNonNull(b, "point B must not be null");
    }

    public static Box of(Point a, Point b) {
        return new Box(a, b);
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
        return "(" + this.a + "," + this.b + ')';
    }

}
