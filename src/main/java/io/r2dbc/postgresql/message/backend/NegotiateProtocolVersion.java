/*
 * Copyright 2026 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static io.r2dbc.postgresql.message.backend.BackendMessageUtils.readCStringUTF8;

/**
 * The NegotiateProtocolVersion message. Sent by the server when the requested minor protocol version is newer than the server supports, or when the client requested unsupported protocol options.
 *
 * @since 1.1.2
 */
public final class NegotiateProtocolVersion implements BackendMessage {

    private final int protocolMinorVersion;

    private final List<String> unsupportedOptions;

    /**
     * Create a new message.
     *
     * @param protocolMinorVersion the newest minor protocol version supported by the server for the major protocol version requested by the client
     * @param unsupportedOptions   the protocol options that were not recognized by the server
     * @throws IllegalArgumentException if {@code unsupportedOptions} is {@code null}
     */
    public NegotiateProtocolVersion(int protocolMinorVersion, List<String> unsupportedOptions) {
        this.protocolMinorVersion = protocolMinorVersion;
        this.unsupportedOptions = Assert.requireNonNull(unsupportedOptions, "unsupportedOptions must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NegotiateProtocolVersion that = (NegotiateProtocolVersion) o;
        return this.protocolMinorVersion == that.protocolMinorVersion &&
            Objects.equals(this.unsupportedOptions, that.unsupportedOptions);
    }

    /**
     * Returns the newest minor protocol version supported by the server for the major protocol version requested by the client.
     *
     * @return the newest minor protocol version supported by the server
     */
    public int getProtocolMinorVersion() {
        return this.protocolMinorVersion;
    }

    /**
     * Returns the protocol options that were not recognized by the server.
     *
     * @return the unsupported protocol options
     */
    public List<String> getUnsupportedOptions() {
        return this.unsupportedOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.protocolMinorVersion, this.unsupportedOptions);
    }

    @Override
    public String toString() {
        return "NegotiateProtocolVersion{" +
            "protocolMinorVersion=" + this.protocolMinorVersion +
            ", unsupportedOptions=" + this.unsupportedOptions +
            '}';
    }

    static NegotiateProtocolVersion decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int protocolMinorVersion = in.readInt();
        int optionCount = in.readInt();

        List<String> unsupportedOptions = new ArrayList<>();
        for (int i = 0; i < optionCount; i++) {
            unsupportedOptions.add(readCStringUTF8(in));
        }

        return new NegotiateProtocolVersion(protocolMinorVersion, Collections.unmodifiableList(unsupportedOptions));
    }

}
