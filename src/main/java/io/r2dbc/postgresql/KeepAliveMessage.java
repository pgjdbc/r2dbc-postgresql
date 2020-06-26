/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.postgresql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.frontend.CopyData;
import io.r2dbc.postgresql.replication.LogSequenceNumber;

/**
 * Nested message for a Replication Stream using {@link CopyData} as carrier.
 */
final class KeepAliveMessage {

    private static final byte KEEP_ALIVE_REPLY = 'r';

    private static final int NO_REPLY_REQUIRED = 0;

    private static final int REPLY_REQUIRED = 1;

    private final LogSequenceNumber received;

    private final LogSequenceNumber flushed;

    private final LogSequenceNumber applied;

    private final long systemClock;

    private boolean replyRequired;

    KeepAliveMessage(LogSequenceNumber received, LogSequenceNumber flushed, LogSequenceNumber applied, long systemClock, boolean replyRequired) {
        this.received = received;
        this.flushed = flushed;
        this.applied = applied;
        this.systemClock = systemClock;
        this.replyRequired = replyRequired;
    }

    public ByteBuf encode(ByteBufAllocator allocator) {

        ByteBuf out = allocator.buffer(34);

        out.writeByte(KEEP_ALIVE_REPLY);
        out.writeLong(this.received.asLong());
        out.writeLong(this.flushed.asLong());
        out.writeLong(this.applied.asLong());
        out.writeLong(this.systemClock);

        if (this.replyRequired) {
            out.writeByte((byte) REPLY_REQUIRED);
        } else {
            out.writeByte(this.received == LogSequenceNumber.INVALID_LSN ? (byte) REPLY_REQUIRED : (byte) NO_REPLY_REQUIRED);
        }

        return out;
    }

}
