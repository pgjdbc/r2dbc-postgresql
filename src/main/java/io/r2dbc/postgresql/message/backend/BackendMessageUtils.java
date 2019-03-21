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
import reactor.util.annotation.Nullable;

import static io.netty.util.CharsetUtil.UTF_8;

final class BackendMessageUtils {

    private static final byte TERMINAL = 0;

    private BackendMessageUtils() {
    }

    static ByteBuf getBody(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        return in.readSlice(in.readInt() - 4);
    }

    @Nullable
    static ByteBuf getEnvelope(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        if (in.readableBytes() < 5) {
            return null;
        }

        int length = 1 + in.getInt(in.readerIndex() + 1);
        if (in.readableBytes() < length) {
            return null;
        }

        return in.readSlice(length);
    }

    static String readCStringUTF8(ByteBuf src) {
        Assert.requireNonNull(src, "src must not be null");

        String s = src.readCharSequence(src.bytesBefore(TERMINAL), UTF_8).toString();
        src.readByte();
        return s;
    }

}
