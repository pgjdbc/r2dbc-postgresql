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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Objects;

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The PasswordMessage message.
 */
public final class PasswordMessage implements FrontendMessage {

    private final String password;

    /**
     * Creates a new message.
     *
     * @param password the password (encrypted, if requested)
     * @throws IllegalArgumentException if {@code password} is {@code null}
     */
    public PasswordMessage(String password) {
        this.password = Assert.requireNonNull(password, "password must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.defer(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();

            writeByte(out, 'p');
            writeLengthPlaceholder(out);
            writeCStringUTF8(out, this.password);

            return Mono.just(writeSize(out));
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PasswordMessage that = (PasswordMessage) o;
        return Objects.equals(this.password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.password);
    }

    @Override
    public String toString() {
        return "PasswordMessage{" +
            "password='" + this.password + '\'' +
            '}';
    }

}
