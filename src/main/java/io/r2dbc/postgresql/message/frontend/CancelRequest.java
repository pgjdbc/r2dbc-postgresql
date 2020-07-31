/*
 * Copyright 2017-2020 the original author or authors.
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

import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeInt;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The CancelRequest message.
 */
public final class CancelRequest implements FrontendMessage {

    private static final int REQUEST_CODE = 80877102;

    private final int processId;

    private final int secretKey;

    /**
     * Create a new message.
     *
     * @param processId the process id of the target backend
     * @param secretKey the secret key for the target backend
     */
    public CancelRequest(int processId, int secretKey) {
        this.processId = processId;
        this.secretKey = secretKey;
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer(16);

            writeLengthPlaceholder(out);
            writeInt(out, REQUEST_CODE);
            writeInt(out, this.processId);
            writeInt(out, this.secretKey);

            return writeSize(out, 0);
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
        CancelRequest that = (CancelRequest) o;
        return this.processId == that.processId &&
            this.secretKey == that.secretKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.processId, this.secretKey);
    }

    @Override
    public String toString() {
        return "CancelRequest{" +
            "processId=" + this.processId +
            ", secretKey=" + this.secretKey +
            '}';
    }

}
