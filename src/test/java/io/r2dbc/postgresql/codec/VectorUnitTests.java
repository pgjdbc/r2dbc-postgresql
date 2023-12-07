/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Vector}.
 */
class VectorUnitTests {

    @Test
    void createsVectorCorrectly() {

        assertThat(Vector.of(1, 2, 3)).isEqualTo(Vector.of(1, 2, 3)).hasSameHashCodeAs(Vector.of(1, 2, 3)).hasToString("[1.0,2.0,3.0]");
        assertThat(Vector.of(1, 2, 3)).isNotEqualTo(Vector.of(2, 1, 3));
        assertThat(Vector.of(Arrays.asList(1, 2, 3))).isEqualTo(Vector.of(1, 2, 3));
    }
}
