/*
 * Copyright 2020 the original author or authors.
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

package io.r2dbc.postgresql;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link EscapeAwareColumnMatcher}.
 */
final class EscapeAwareColumnMatcherUnitTests {

    List<String> columns = Arrays.asList("one", "two", "three", "one");

    @Test
    void containsConsidersNamingRules() {

        assertThat(EscapeAwareColumnMatcher.findColumn("one", columns)).isNotNull();
        assertThat(EscapeAwareColumnMatcher.findColumn("\"one\"", columns)).isNotNull();
        assertThat(EscapeAwareColumnMatcher.findColumn("\"one", columns)).isNull();
        assertThat(EscapeAwareColumnMatcher.findColumn("one\"", columns)).isNull();
        assertThat(EscapeAwareColumnMatcher.findColumn("\"One\"", columns)).isNull();
    }

}
