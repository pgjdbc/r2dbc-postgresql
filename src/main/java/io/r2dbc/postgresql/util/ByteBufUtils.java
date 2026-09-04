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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
     * Combine the buffers emitted by the given {@link Publisher} into a single {@link ByteBuf}, taking ownership of the emitted buffers. A {@link Mono} source emits at most one
     * buffer and is passed through without aggregation machinery (issue #735); other sources are aggregated into a {@link CompositeByteBuf}. An empty source yields a zero-length
     * buffer.
     *
     * @param buffers   the {@link Publisher} of {@link ByteBuf}s to combine
     * @param allocator the {@link ByteBufAllocator} to use if a buffer needs to be allocated
     * @return the combined {@link ByteBuf}, never {@link Unpooled#EMPTY_BUFFER} as callers use that instance as a SQL NULL sentinel
     * @throws IllegalArgumentException if {@code buffers} or {@code allocator} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public static Mono<ByteBuf> combine(Publisher<? extends ByteBuf> buffers, ByteBufAllocator allocator) {
        Assert.requireNonNull(buffers, "buffers must not be null");
        Assert.requireNonNull(allocator, "allocator must not be null");

        if (buffers instanceof Mono) {
            return ((Mono<ByteBuf>) buffers)
                .defaultIfEmpty(Unpooled.EMPTY_BUFFER)
                .map(buffer -> buffer != Unpooled.EMPTY_BUFFER ? buffer : allocator.buffer(0));
        }

        return Flux.from(buffers).<ByteBuf>reduceWith(allocator::compositeBuffer, (composite, buffer) -> ((CompositeByteBuf) composite).addComponent(true, buffer));
    }

}
