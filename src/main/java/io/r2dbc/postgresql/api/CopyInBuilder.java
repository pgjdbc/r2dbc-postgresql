/*
 * Copyright 2022 the original author or authors.
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

package io.r2dbc.postgresql.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.message.frontend.CopyData;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * Interface specifying a builder contract to configure a {@code COPY FROM STDIN} operation.
 *
 * @since 1.0
 */
public interface CopyInBuilder {

    /**
     * Postgres parse limit for large messages {@code 2^30 - 1} bytes.
     */
    int MAX_FRAME_SIZE = 0x3fffffff - 1;

    /**
     * Configure a {@link Publisher} emitting publishers of buffers that to write data to a {@link CopyData} frame per emitted publisher.
     * This method allows controlling flush behavior and chunking of buffers. The provided stream must ensure to not exceed size limits ({@link #MAX_FRAME_SIZE}) of the {@link CopyData} frame.
     * <p>If a provided publisher terminates with an error signal then the copy operation terminates with a failure and gets cancelled on the server.
     *
     * @param stdin the bytes to write to a {@link CopyData} frame.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    CopyInBuilder fromMany(Publisher<? extends Publisher<ByteBuf>> stdin);

    /**
     * Configure a {@link Publisher} emitting buffers that are written to a single {@link CopyData} frame.
     * If the total amount of data to be written exceeds the copy frame size limitation ({@link #MAX_FRAME_SIZE}), then use {@link #fromMany(Publisher)} to split up the input data to many
     * {@link CopyData} frames.
     * <p>If the provided publisher terminates with an error signal then the copy operation terminates with a failure and gets cancelled on the server.
     *
     * @param stdin the bytes to write to a {@link CopyData} frame.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    default CopyInBuilder from(Publisher<ByteBuf> stdin) {
        return fromMany(Mono.just(stdin));
    }

    /**
     * Configure an input buffer that is written to a single {@link CopyData} frame.
     *
     * @param stdin the bytes to write to a {@link CopyData} frame.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    default CopyInBuilder from(ByteBuf stdin) {
        return from(Mono.just(stdin));
    }

    /**
     * Configure an input buffer that is written to a single {@link CopyData} frame.
     *
     * @param stdin the bytes to write to a {@link CopyData} frame.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    default CopyInBuilder from(ByteBuffer stdin) {
        return from(Unpooled.wrappedBuffer(stdin));
    }

    /**
     * Configure an input buffer that is written to a single {@link CopyData} frame.
     *
     * @param stdin the bytes to write to a {@link CopyData} frame.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    default CopyInBuilder from(byte[] stdin) {
        return from(Unpooled.wrappedBuffer(stdin));
    }

    /**
     * Configure an input buffer along with {@code offset} and {@code length} whose specified chunk is written to a single {@link CopyData} frame.
     *
     * @param stdin  the bytes to write to a {@link CopyData} frame.
     * @param offset the start offset in the data.
     * @param length the number of bytes to write.
     * @return {@code this} {@link CopyInBuilder builder}.
     */
    default CopyInBuilder from(byte[] stdin, int offset, int length) {
        return from(Unpooled.wrappedBuffer(stdin, offset, length));
    }

    /**
     * Build the final publisher that initiates the {@code COPY} operation. The copy data messages sent to the server are triggered by the provided input buffer.
     * Cancelling the copy operation sends a failure frame to the server to terminate the copy operation with an error.
     *
     * @return the publisher that initiates the {@code COPY} operation upon subscription.
     */
    Mono<Long> build();

}
