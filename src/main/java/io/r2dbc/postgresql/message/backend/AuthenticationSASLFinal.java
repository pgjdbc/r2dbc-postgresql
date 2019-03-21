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
import io.r2dbc.postgresql.util.Assert;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The AuthenticationSASLFinal message.
 */
public final class AuthenticationSASLFinal implements AuthenticationMessage {

    private final ByteBuffer additionalData;

    /**
     * Creates a new message.
     *
     * @param additionalData SASL outcome "additional data", specific to the SASL mechanism being used
     * @throws IllegalArgumentException if {@code additionalData} is {@code null}
     */
    public AuthenticationSASLFinal(ByteBuf additionalData) {
        Assert.requireNonNull(additionalData, "additionalData must not be null");

        this.additionalData = additionalData.nioBuffer();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticationSASLFinal that = (AuthenticationSASLFinal) o;
        return Objects.equals(this.additionalData, that.additionalData);
    }

    /**
     * Returns SASL outcome "additional data", specific to the SASL mechanism being used.
     *
     * @return SASL outcome "additional data", specific to the SASL mechanism being used
     */
    public ByteBuffer getAdditionalData() {
        return this.additionalData;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.additionalData);
    }

    @Override
    public String toString() {
        return "AuthenticationSASLFinal{" +
            "additionalData=" + this.additionalData +
            '}';
    }

    static AuthenticationSASLFinal decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        return new AuthenticationSASLFinal(in.readSlice(in.readableBytes()));
    }

}
