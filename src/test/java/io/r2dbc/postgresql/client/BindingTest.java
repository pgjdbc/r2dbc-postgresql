/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class BindingTest {

    @Test
    void addNoIdentifier() {
        assertThatNullPointerException().isThrownBy(() -> new Binding().add(null, new Parameter(TEXT, 100, null)))
            .withMessage("identifier must not be null");
    }

    @Test
    void addNoParameter() {
        assertThatNullPointerException().isThrownBy(() -> new Binding().add(1, null))
            .withMessage("parameter must not be null");
    }

    @Test
    void empty() {
        Binding binding = new Binding();

        assertThat(binding.getParameterFormats()).isEmpty();
    }

    @Test
    void getParameterFormats() {
        Binding binding = new Binding();
        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThat(binding.getParameterFormats()).containsExactly(BINARY, TEXT, BINARY);
    }

    @Test
    void getParameterTypes() {
        Binding binding = new Binding();
        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThat(binding.getParameterTypes()).containsExactly(100, UNSPECIFIED.getObjectId(), 100);
    }

    @Test
    void getParameterValues() {
        Binding binding = new Binding();
        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThat(binding.getParameterValues()).containsExactly(TEST.buffer(4).writeInt(200), null, TEST.buffer(4).writeInt(300));
    }

}
