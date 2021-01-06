/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.util.ByteBufUtils;
import io.r2dbc.postgresql.util.ByteBufferUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Json}.
 */
class JsonTestUnitTests {

    static Stream<Json> valuesToConsume() {

        return Stream.of(Json.of(ByteBuffer.wrap("hello-world".getBytes())),
            Json.of(Unpooled.wrappedBuffer("hello-world".getBytes())),
            Json.of(new ByteArrayInputStream("hello-world".getBytes())),
            Json.of("hello-world"),
            Json.of("hello-world".getBytes()),
            new Json.JsonOutput(Unpooled.wrappedBuffer("hello-world".getBytes())));
    }

    @MethodSource("valuesToConsume")
    @ParameterizedTest
    void testConsumeByteBuffer(Json json) {
        assertThat(json.mapBuffer(ByteBufferUtils::decode)).isEqualTo("hello-world");
    }

    @MethodSource("valuesToConsume")
    @ParameterizedTest
    void testConsumeByteBuf(Json json) {
        assertThat(json.mapByteBuf(ByteBufUtils::decode)).isEqualTo("hello-world");
    }

    @MethodSource("valuesToConsume")
    @ParameterizedTest
    void testConsumeInputStream(Json json) {

        byte[] bytes = json.mapInputStream(inputStream -> {

            try {
                return StreamUtils.copyToByteArray(inputStream);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

        });

        assertThat(bytes).isEqualTo("hello-world".getBytes());
    }

    @MethodSource("valuesToConsume")
    @ParameterizedTest
    void testConsumeByteArray(Json json) {
        assertThat(json.asArray()).isEqualTo("hello-world".getBytes());
    }

    @MethodSource("valuesToConsume")
    @ParameterizedTest
    void testConsumeAsString(Json json) {
        assertThat(json.asString()).isEqualTo("hello-world");
    }

}
