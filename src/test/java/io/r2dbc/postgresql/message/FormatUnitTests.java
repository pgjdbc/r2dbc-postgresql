/*
 * Copyright 2017 the original author or authors.
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

package io.r2dbc.postgresql.message;

import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link Format}.
 */
final class FormatUnitTests {

    @Test
    void getDiscriminatorBinary() {
        assertThat(FORMAT_BINARY.getDiscriminator()).isEqualTo((byte) 1);
    }

    @Test
    void getDiscriminatorText() {
        assertThat(FORMAT_TEXT.getDiscriminator()).isEqualTo((byte) 0);
    }

    @Test
    void valueOfBinary() {
        assertThat(Format.valueOf((short) 1)).isEqualTo(FORMAT_BINARY);
    }

    @Test
    void valueOfInvalid() {
        assertThatIllegalArgumentException().isThrownBy(() -> Format.valueOf((short) -1))
            .withMessage("-1 is not a valid format");
    }

    @Test
    void valueOfText() {
        assertThat(Format.valueOf((short) 0)).isEqualTo(FORMAT_TEXT);
    }

}
