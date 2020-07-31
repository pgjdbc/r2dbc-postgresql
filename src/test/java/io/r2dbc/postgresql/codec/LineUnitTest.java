package io.r2dbc.postgresql.codec;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Line}.
 */
final class LineUnitTest {

    @Test
    void createByTwoPoints() {

        assertThat(Line.of(-1.5, -1, 4.5)).isEqualTo(Line.of(Point.of(3, 0), Point.of(1, 3)));
        assertThat(Line.of(-1, 0, 8)).isEqualTo(Line.of(1, 7, 1, 3));
    }

}
