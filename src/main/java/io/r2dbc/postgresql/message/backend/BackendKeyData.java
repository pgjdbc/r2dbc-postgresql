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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.util.Assert;

import java.util.Arrays;
import java.util.Objects;

/**
 * The BackendKeyData message.
 */
public final class BackendKeyData implements BackendMessage {

    private final int processId;

    private final byte[] secretKey;

    /**
     * Create a new message.
     *
     * @param processId the process ID of this backend
     * @param secretKey the secret key of this backend. Always 4 bytes before protocol version 3.2, variable-length (up to 256 bytes) since 3.2
     * @throws IllegalArgumentException if {@code secretKey} is {@code null}
     */
    public BackendKeyData(int processId, byte[] secretKey) {
        this.processId = processId;
        this.secretKey = Assert.requireNonNull(secretKey, "secretKey must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BackendKeyData that = (BackendKeyData) o;
        return this.processId == that.processId &&
            Arrays.equals(this.secretKey, that.secretKey);
    }

    /**
     * Returns the process ID of this backend.
     *
     * @return the process ID of this backend
     */
    public int getProcessId() {
        return this.processId;
    }

    /**
     * Returns the secret key of this backend.
     *
     * @return the secret key of this backend
     */
    public byte[] getSecretKey() {
        return this.secretKey;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(this.processId) + Arrays.hashCode(this.secretKey);
    }

    @Override
    public String toString() {
        return "BackendKeyData{" +
            "processId=" + this.processId +
            ", secretKey=" + Arrays.toString(this.secretKey) +
            '}';
    }

    static BackendKeyData decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        int processId = in.readInt();
        byte[] secretKey = new byte[in.readableBytes()];
        in.readBytes(secretKey);

        return new BackendKeyData(processId, secretKey);
    }

}
