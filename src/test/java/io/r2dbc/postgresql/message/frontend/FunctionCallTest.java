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

package io.r2dbc.postgresql.message.frontend;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageAssert.assertThat;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class FunctionCallTest {

    @Test
    void constructorNoArgumentFormats() {
        assertThatNullPointerException().isThrownBy(() -> new FunctionCall(null, Collections.singletonList(TEST.buffer(4).writeInt(100)), 200, FORMAT_BINARY))
            .withMessage("argumentFormats must not be null");
    }

    @Test
    void constructorNoArguments() {
        assertThatNullPointerException().isThrownBy(() -> new FunctionCall(Collections.singletonList(FORMAT_BINARY), null, 200, FORMAT_BINARY))
            .withMessage("arguments must not be null");
    }

    @Test
    void constructorNoResultFormat() {
        assertThatNullPointerException().isThrownBy(() -> new FunctionCall(Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), 200, null))
            .withMessage("resultFormat must not be null");
    }

    @Test
    void encode() {
        assertThat(new FunctionCall(Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), 200, FORMAT_BINARY)).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> buffer
                .writeByte('F')
                .writeInt(24)
                .writeInt(200)
                .writeShort(1)
                .writeShort(1)
                .writeShort(1)
                .writeInt(4)
                .writeInt(100)
                .writeShort(1));
    }

    @Test
    void encodeNullArgument() {
        assertThat(new FunctionCall(Collections.singletonList(FORMAT_BINARY), Collections.singletonList(null), 200, FORMAT_BINARY)).encoded()
            .isDeferred()
            .isEncodedAs(buffer -> buffer
                .writeByte('F')
                .writeInt(20)
                .writeInt(200)
                .writeShort(1)
                .writeShort(1)
                .writeShort(1)
                .writeInt(-1)
                .writeShort(1));
    }

}
