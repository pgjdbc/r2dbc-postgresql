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

package io.r2dbc.postgresql.message.frontend;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageAssert.assertThat;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BindTest {

    @Test
    void constructorNoNames() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Bind(null, Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)),
            Collections.singletonList(FORMAT_BINARY), "test-source"))
            .withMessage("name must not be null");
    }

    @Test
    void constructorNoParameterFormats() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Bind("test-name", null, Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.singletonList(FORMAT_BINARY), "test" +
            "-source"))
            .withMessage("parameterFormats must not be null");
    }

    @Test
    void constructorNoParameters() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Bind("test-name", Collections.singletonList(FORMAT_BINARY), null, Collections.singletonList(FORMAT_BINARY), "test-source"))
            .withMessage("parameters must not be null");
    }

    @Test
    void constructorNoResultFormats() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Bind("test-name", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), null, "test" +
            "-source"))
            .withMessage("resultFormats must not be null");
    }

    @Test
    void constructorNoSource() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Bind("test-name", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)),
            Collections.singletonList(FORMAT_BINARY), null))
            .withMessage("source must not be null");
    }

    @Test
    void encode() {
        assertThat(new Bind("test-name", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.singletonList(FORMAT_BINARY), "test-source")).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> {
                buffer
                    .writeByte('B')
                    .writeInt(44);

                buffer.writeCharSequence("test-name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-source", UTF_8);
                buffer.writeByte(0);

                buffer
                    .writeShort(1)
                    .writeShort(1)
                    .writeShort(1)
                    .writeInt(4)
                    .writeInt(100)
                    .writeShort(1)
                    .writeShort(1);

                return buffer;
            });
    }

    @Test
    void encodeNullParameter() {
        assertThat(new Bind("test-name", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(null), Collections.singletonList(FORMAT_BINARY), "test-source")).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> {
                buffer
                    .writeByte('B')
                    .writeInt(40);

                buffer.writeCharSequence("test-name", UTF_8);
                buffer.writeByte(0);

                buffer.writeCharSequence("test-source", UTF_8);
                buffer.writeByte(0);

                buffer
                    .writeShort(1)
                    .writeShort(1)
                    .writeShort(1)
                    .writeInt(-1)
                    .writeShort(1)
                    .writeShort(1);

                return buffer;
            });

    }

}
