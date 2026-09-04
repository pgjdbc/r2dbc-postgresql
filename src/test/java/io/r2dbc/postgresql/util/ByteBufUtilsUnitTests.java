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

package io.r2dbc.postgresql.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ByteBufUtils}.
 */
final class ByteBufUtilsUnitTests {

    @Test
    void shouldCopyByteBuffer() {

        ByteBuf source = Unpooled.wrappedBuffer("hello-world".getBytes());
        ByteBuffer byteBuffer = ByteBufferUtils.toByteBuffer(source);

        assertThat(byteBuffer).isEqualTo(ByteBuffer.wrap("hello-world".getBytes()));
    }

    @Test
    void combineShouldPassThroughSingleMonoBuffer() {

        ByteBuf buffer = ByteBufUtils.encode(TEST, "hello-world");

        assertThat(ByteBufUtils.combine(Mono.just(buffer), TEST).block()).isSameAs(buffer);

        buffer.release();
    }

    @Test
    void combineShouldEmitZeroLengthBufferForEmptyMono() {

        ByteBuf combined = ByteBufUtils.combine(Mono.empty(), TEST).block();

        assertThat(combined.readableBytes()).isZero();
        assertThat(combined).isNotSameAs(Unpooled.EMPTY_BUFFER);

        combined.release();
    }

    @Test
    void combineShouldNotEmitEmptyBufferSentinelForMonoSource() {

        ByteBuf combined = ByteBufUtils.combine(Mono.just(Unpooled.EMPTY_BUFFER), TEST).block();

        assertThat(combined.readableBytes()).isZero();
        assertThat(combined).isNotSameAs(Unpooled.EMPTY_BUFFER);

        combined.release();
    }

    @Test
    void combineShouldConcatenateFluxBuffers() {

        ByteBuf first = ByteBufUtils.encode(TEST, "hello");
        ByteBuf second = ByteBufUtils.encode(TEST, "-");
        ByteBuf third = ByteBufUtils.encode(TEST, "world");

        ByteBuf combined = ByteBufUtils.combine(Flux.just(first, second, third), TEST).block();

        assertThat(combined.readableBytes()).isEqualTo(11);
        assertThat(ByteBufUtils.decode(combined.duplicate())).isEqualTo("hello-world");

        assertThat(combined.release()).isTrue();
        assertThat(first.refCnt()).isZero();
        assertThat(second.refCnt()).isZero();
        assertThat(third.refCnt()).isZero();
    }

    @Test
    void combineShouldEmitZeroLengthBufferForEmptyFlux() {

        ByteBuf combined = ByteBufUtils.combine(Flux.empty(), TEST).block();

        assertThat(combined.readableBytes()).isZero();
        assertThat(combined).isNotSameAs(Unpooled.EMPTY_BUFFER);

        combined.release();
    }

    @Test
    void combineShouldRejectNullArguments() {

        assertThatIllegalArgumentException().isThrownBy(() -> ByteBufUtils.combine(null, TEST));
        assertThatIllegalArgumentException().isThrownBy(() -> ByteBufUtils.combine(Mono.empty(), null));
    }

}
