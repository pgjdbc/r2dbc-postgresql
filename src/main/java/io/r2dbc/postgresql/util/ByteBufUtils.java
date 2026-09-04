/*
 * Copyright 2017 the original author or authors.
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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Utilities for working with {@link ByteBuf}s.
 */
public final class ByteBufUtils {

    private ByteBufUtils() {
    }

    /**
     * Decode a {@link ByteBuf} into a {@link String}.
     *
     * @param byteBuf the {@link ByteBuf} to decode
     * @return the {@link String} decoded from the {@link ByteBuf}
     * @throws IllegalArgumentException if {@code byteBuf} is {@code null}
     */
    public static String decode(ByteBuf byteBuf) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");

        return byteBuf.readCharSequence(byteBuf.readableBytes(), UTF_8).toString();
    }

    /**
     * Encode a {@link CharSequence} into a {@link ByteBuf}.
     *
     * @param byteBufAllocator the {@link ByteBufAllocator} to use to create a buffer
     * @param s                the {@link CharSequence} to encode
     * @return the {@link ByteBuf} with the {@link CharSequence} encoded within it
     * @throws IllegalArgumentException if {@code byteBufAllocator} or {@code s} is {@code null}
     */
    public static ByteBuf encode(ByteBufAllocator byteBufAllocator, CharSequence s) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        Assert.requireNonNull(s, "s must not be null");

        ByteBuf byteBuf = byteBufAllocator.buffer();
        byteBuf.writeCharSequence(s, UTF_8);
        return byteBuf;
    }

    /**
     * Combine the given {@link ByteBuf}s into a single {@link ByteBuf}, taking ownership of the given buffers. A single buffer is returned as-is to avoid allocating a
     * {@link CompositeByteBuf} for the common single-buffer case (issue #735).
     *
     * @param buffers   the {@link ByteBuf}s to combine
     * @param allocator the {@link ByteBufAllocator} to use if a buffer needs to be allocated
     * @return the combined {@link ByteBuf}, never {@link Unpooled#EMPTY_BUFFER} as callers use that instance as a SQL NULL sentinel
     * @throws IllegalArgumentException if {@code buffers} or {@code allocator} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static ByteBuf combine(List<? extends ByteBuf> buffers, ByteBufAllocator allocator) {
        Assert.requireNonNull(buffers, "buffers must not be null");
        Assert.requireNonNull(allocator, "allocator must not be null");

        if (buffers.size() == 1) {
            ByteBuf buffer = buffers.get(0);
            return buffer != Unpooled.EMPTY_BUFFER ? buffer : allocator.buffer(0);
        }

        if (buffers.isEmpty()) {
            return allocator.buffer(0);
        }

        CompositeByteBuf composite = allocator.compositeBuffer(buffers.size());
        composite.addComponents(true, (Iterable<ByteBuf>) buffers);
        return composite;
    }

}
