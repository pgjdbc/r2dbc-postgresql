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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;

/**
 * A message sent from a frontend client to a backend server.
 */
public interface FrontendMessage {

    /**
     * Encode a message into a {@link ByteBuf}.
     *
     * @param byteBufAllocator the byteBufAllocator to use to get a {@link ByteBuf} to write into
     * @return a {@link Publisher} that produces the {@link ByteBuf} containing the encoded message
     * @throws IllegalArgumentException if {@code byteBufAllocator} is {@code null}
     */
    Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator);

    /**
     * Release any resources (such as bound parameter buffers) held by this message without encoding it. Invoked when a
     * message is discarded before reaching the wire, e.g. when the connection is closed or the conversation is cancelled
     * while the message is still queued. The default implementation does nothing. Implementations that own
     * reference-counted buffers must release them here and must be idempotent so that disposal is safe regardless of
     * whether the message was already encoded.
     */
    default void dispose() {
    }

    /**
     * Interface for messages that can be directly encoded without producing a {@link Publisher} first.
     */
    interface DirectEncoder {

        /**
         * Encode a message directly by writing its content to  a {@link ByteBuf}.
         *
         * @param byteBuf the target {@link ByteBuf} to write into
         */
        void encode(ByteBuf byteBuf);

    }

}
