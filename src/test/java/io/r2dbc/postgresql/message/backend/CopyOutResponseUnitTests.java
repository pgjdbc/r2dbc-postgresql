/*
 * Copyright 2017-2020 the original author or authors.
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

package io.r2dbc.postgresql.message.backend;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link CopyOutResponse}.
 */
final class CopyOutResponseUnitTests {

    @Test
    void constructorNoColumnFormats() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CopyOutResponse(null, FORMAT_BINARY))
            .withMessage("columnFormats must not be null");
    }

    @Test
    void constructorNoOverallFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CopyOutResponse(null, FORMAT_BINARY))
            .withMessage("columnFormats must not be null");
    }

    @Test
    void decode() {
        assertThat(CopyOutResponse.class)
            .decoded(buffer -> buffer
                .writeByte(1)
                .writeShort(1)
                .writeShort(1))
            .isEqualTo(new CopyOutResponse(EnumSet.of(FORMAT_BINARY), FORMAT_BINARY));
    }

    @Test
    void decodeOverallFormatText() {
        assertThat(CopyOutResponse.class)
            .decoded(buffer -> buffer
                .writeByte(0)
                .writeShort(2)
                .writeShort(0)
                .writeShort(0))
            .isEqualTo(new CopyOutResponse(Collections.unmodifiableSet(EnumSet.of(FORMAT_TEXT)), FORMAT_TEXT));
    }

}
