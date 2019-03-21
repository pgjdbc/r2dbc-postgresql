/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ParameterTest {

    @Test
    void constructorNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Parameter(null, 100, null))
            .withMessage("format must not be null");
    }

    @Test
    void constructorNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Parameter(FORMAT_TEXT, null, null))
            .withMessage("type must not be null");
    }

    @Test
    void getters() {
        Parameter parameter = new Parameter(FORMAT_TEXT, 100, TEST.buffer(4).writeInt(200));

        assertThat(parameter.getFormat()).isEqualTo(FORMAT_TEXT);
        assertThat(parameter.getType()).isEqualTo(100);
        assertThat(parameter.getValue()).isEqualTo(TEST.buffer(4).writeInt(200));
    }

}
