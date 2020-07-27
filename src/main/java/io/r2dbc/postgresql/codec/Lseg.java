package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

/**
 * Value object that maps to the {@code lseg} datatype in Postgres.
 * <p>
 * Uses {@code double} to represent the coordinates.
 */
public final class Lseg {

    private final Point p1;

    private final Point p2;

    private Lseg(Point p1, Point p2) {
        this.p1 = Assert.requireNonNull(p1, "p1 must not be null");
        this.p2 = Assert.requireNonNull(p2, "p2 must not be null");
    }

    public static Lseg of(Point p1, Point p2) {
        return new Lseg(p1, p2);
    }

    public Point getP1() {
        return this.p1;
    }

    public Point getP2() {
        return this.p2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lseg lseg = (Lseg) o;
        return this.p1.equals(lseg.p1) &&
            this.p2.equals(lseg.p2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.p1, this.p2);
    }

    @Override
    public String toString() {
        return "[" + this.p1 + "," + this.p2 + ']';
    }

}
