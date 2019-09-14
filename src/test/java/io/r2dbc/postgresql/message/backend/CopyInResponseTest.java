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

package io.r2dbc.postgresql.message.backend;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class CopyInResponseTest {

    @Test
    void constructorNoColumnFormats() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CopyInResponse(Collections.singletonList(FORMAT_BINARY), null))
            .withMessage("overallFormat must not be null");
    }

    @Test
    void constructorNoOverallFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CopyInResponse(null, FORMAT_BINARY))
            .withMessage("columnFormats must not be null");
    }

    @Test
    void decode() {
        assertThat(CopyInResponse.class)
            .decoded(buffer -> buffer
                .writeByte(1)
                .writeShort(1)
                .writeShort(1))
            .isEqualTo(new CopyInResponse(EnumSet.of(FORMAT_BINARY), FORMAT_BINARY));
    }

}
