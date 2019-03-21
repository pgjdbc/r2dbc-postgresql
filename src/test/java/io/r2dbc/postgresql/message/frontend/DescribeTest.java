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

package io.r2dbc.postgresql.message.frontend;

import org.junit.jupiter.api.Test;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.PORTAL;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.STATEMENT;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class DescribeTest {

    @Test
    void constructorNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Describe(null, STATEMENT))
            .withMessage("name must not be null");
    }

    @Test
    void constructorNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Describe("test-name", null))
            .withMessage("type must not be null");
    }

    @Test
    void encode() {
        assertThat(new Describe("test-name", PORTAL)).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> {
                buffer
                    .writeByte('D')
                    .writeInt(15)
                    .writeByte('P');

                buffer.writeCharSequence("test-name", UTF_8);
                buffer.writeByte(0);

                return buffer;
            });
    }

}
