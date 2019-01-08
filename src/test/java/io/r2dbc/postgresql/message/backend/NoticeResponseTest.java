/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.message.backend;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class NoticeResponseTest {

    @Test
    void constructorNoFields() {
        assertThatIllegalArgumentException().isThrownBy(() -> new NoticeResponse(null))
            .withMessage("fields must not be null");
    }

    @Test
    void decode() {
        assertThat(NoticeResponse.class)
            .decoded(buffer -> {
                buffer.writeByte('C');

                buffer.writeCharSequence("test-value", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new NoticeResponse(Collections.singletonList(new Field(CODE, "test-value"))));
    }

}
