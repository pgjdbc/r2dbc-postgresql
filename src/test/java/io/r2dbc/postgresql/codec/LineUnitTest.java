package io.r2dbc.postgresql.codec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Line}.
 */
class LineUnitTest {

    @Test
    void createByTwoPoints() {
        Assertions.assertEquals(Line.of(-1.5, -1, 4.5), Line.of(Point.of(3, 0), Point.of(1, 3)));

        Assertions.assertEquals(Line.of(-1, 0, 8), Line.of(1, 7, 1, 3));
    }

}