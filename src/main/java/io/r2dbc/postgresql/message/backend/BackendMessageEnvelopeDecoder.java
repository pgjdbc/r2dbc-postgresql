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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.r2dbc.postgresql.message.backend.BackendMessageUtils.getEnvelope;

/**
 * Envelope decoder for inbound {@link BackendMessage}s.
 */
public final class BackendMessageEnvelopeDecoder implements Function<ByteBuf, Publisher<CompositeByteBuf>> {

    private final CompositeByteBuf byteBuf;

    private final AtomicBoolean disposed = new AtomicBoolean();

    public BackendMessageEnvelopeDecoder(ByteBufAllocator allocator) {
        this.byteBuf = allocator.compositeBuffer();
    }

    /**
     * Splits a {@link ByteBuf} into a {@link Flux} of {@link CompositeByteBuf}s contains exactly one backend envelope.
     * If the {@link ByteBuf} does not end on a {@link BackendMessage} boundary, the {@link ByteBuf} will be retained until
     * an the concatenated contents of all retained {@link ByteBuf}s is a {@link BackendMessage} boundary.
     *
     * @param in the {@link ByteBuf} to decode
     * @return a {@link Flux} of {@link BackendMessage}s
     */
    @Override
    public Flux<CompositeByteBuf> apply(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        this.byteBuf.addComponent(true, in);
        this.byteBuf.retain();

        return Flux.<CompositeByteBuf>create(sink -> {
            try {
                CompositeByteBuf envelope = getEnvelope(this.byteBuf);
                while (envelope != null) {
                    sink.next(envelope);
                    envelope = getEnvelope(this.byteBuf);
                }
                sink.complete();
            } finally {
                this.byteBuf.discardReadComponents();
            }
        })
            .doFinally(s -> ReferenceCountUtil.release(this.byteBuf));
    }

    public void dispose() {
        if (this.disposed.compareAndSet(false, true)) {
            ReferenceCountUtil.release(this.byteBuf);
        }
    }
}
