/*
 * Copyright 2020 the original author or authors.
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
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * {@link FrontendMessage} that aggregates {@link FrontendMessage}, specifically {@link DirectEncoder} messages to send multiple {@link FrontendMessage} in a single TCP packet.
 *
 * @since 0.8.6
 */
public final class CompositeFrontendMessage implements FrontendMessage, FrontendMessage.DirectEncoder {

    private final List<DirectEncoder> messages;

    public CompositeFrontendMessage(DirectEncoder... messages) {
        this(Arrays.asList(messages));
    }

    public CompositeFrontendMessage(List<DirectEncoder> messages) {
        this.messages = messages;
    }

    public boolean contains(FrontendMessage message) {
        return this.messages.contains(message);
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        return Mono.fromSupplier(() -> {

            ByteBuf buffer = byteBufAllocator.buffer();
            encode(buffer);

            return buffer;
        });
    }

    @Override
    public void encode(ByteBuf byteBuf) {

        for (DirectEncoder message : this.messages) {
            message.encode(byteBuf);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CompositeFrontendMessage)) {
            return false;
        }
        CompositeFrontendMessage that = (CompositeFrontendMessage) o;
        return Objects.equals(this.messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.messages);
    }

    @Override
    public String toString() {
        return this.messages.toString();
    }

}
