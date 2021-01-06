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
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.MESSAGE_OVERHEAD;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeArray;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The Termination message.
 */
public final class Terminate implements FrontendMessage, FrontendMessage.DirectEncoder {

    /**
     * A static singleton instance that should always be used.
     */
    public static final Terminate INSTANCE = new Terminate();

    private final byte[] message = writeArray(buffer -> {

        writeByte(buffer, 'X');
        writeLengthPlaceholder(buffer);

        return writeSize(buffer);
    });

    private Terminate() {
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer(MESSAGE_OVERHEAD);

            encode(out);
            return out;
        });
    }

    @Override
    public void encode(ByteBuf byteBuf) {
        byteBuf.writeBytes(this.message);
    }

    @Override
    public String toString() {
        return "Terminate{}";
    }

}
