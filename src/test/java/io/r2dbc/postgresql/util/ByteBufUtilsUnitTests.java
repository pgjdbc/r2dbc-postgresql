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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    void combineShouldReturnSingleBufferAsIs() {

        ByteBuf buffer = ByteBufUtils.encode(TEST, "hello-world");

        assertThat(ByteBufUtils.combine(Collections.singletonList(buffer), TEST)).isSameAs(buffer);

        buffer.release();
    }

    @Test
    void combineShouldConcatenateMultipleBuffers() {

        ByteBuf first = ByteBufUtils.encode(TEST, "hello");
        ByteBuf second = ByteBufUtils.encode(TEST, "-");
        ByteBuf third = ByteBufUtils.encode(TEST, "world");

        ByteBuf combined = ByteBufUtils.combine(Arrays.asList(first, second, third), TEST);

        assertThat(combined.readableBytes()).isEqualTo(11);
        assertThat(ByteBufUtils.decode(combined.duplicate())).isEqualTo("hello-world");

        assertThat(combined.release()).isTrue();
        assertThat(first.refCnt()).isZero();
        assertThat(second.refCnt()).isZero();
        assertThat(third.refCnt()).isZero();
    }

    @Test
    void combineShouldReturnEmptyBufferForEmptyList() {

        ByteBuf combined = ByteBufUtils.combine(Collections.<ByteBuf>emptyList(), TEST);

        assertThat(combined.readableBytes()).isZero();
        assertThat(combined).isNotSameAs(Unpooled.EMPTY_BUFFER);

        combined.release();
    }

    @Test
    void combineShouldNotReturnEmptyBufferSentinelForSingleEmptyBuffer() {

        ByteBuf combined = ByteBufUtils.combine(Collections.singletonList(Unpooled.EMPTY_BUFFER), TEST);

        assertThat(combined.readableBytes()).isZero();
        assertThat(combined).isNotSameAs(Unpooled.EMPTY_BUFFER);

        combined.release();
    }

    @Test
    void combineShouldRejectNullArguments() {

        List<ByteBuf> buffers = Collections.emptyList();

        assertThatIllegalArgumentException().isThrownBy(() -> ByteBufUtils.combine(null, TEST));
        assertThatIllegalArgumentException().isThrownBy(() -> ByteBufUtils.combine(buffers, null));
    }

}
