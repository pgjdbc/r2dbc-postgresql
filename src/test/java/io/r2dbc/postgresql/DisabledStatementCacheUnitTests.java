/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.postgresql.client.Binding;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DisabledStatementCache}.
 */
class DisabledStatementCacheUnitTests {

    DisabledStatementCache cache = new DisabledStatementCache();

    @Test
    void getName() {

        assertThat(this.cache.getName(new Binding(0), "foo")).isEmpty();
        assertThat(this.cache.getName(new Binding(0), "bar")).isEmpty();
    }

    @Test
    void requiresPrepare() {

        assertThat(this.cache.requiresPrepare(new Binding(0), "foo")).isTrue();
        this.cache.put(new Binding(0), "foo", "S_1");
        assertThat(this.cache.requiresPrepare(new Binding(0), "foo")).isTrue();
    }

}
