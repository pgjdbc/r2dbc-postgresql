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

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class RowDescriptionTest {

    @Test
    void constructorNoFields() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RowDescription(null))
            .withMessage("fields must not be null");
    }

    @Test
    void decode() {
        assertThat(RowDescription.class)
            .decoded(buffer -> {
                buffer.writeShort(1);

                buffer.writeCharSequence("test-name", UTF_8);
                buffer.writeByte(0);

                buffer
                    .writeInt(500)
                    .writeShort(100)
                    .writeInt(200)
                    .writeShort(400)
                    .writeInt(300)
                    .writeShort(1);

                return buffer;
            })
            .isEqualTo(new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_BINARY, "test-name", 500))));
    }


    @Test
    void fiedlConstructorNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RowDescription.Field((short) 100, 200, 300, (short) 400, null, "test-name", 500))
            .withMessage("format must not be null");
    }

    @Test
    void fieldConstructorNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_BINARY, null, 500))
            .withMessage("name must not be null");
    }

}
