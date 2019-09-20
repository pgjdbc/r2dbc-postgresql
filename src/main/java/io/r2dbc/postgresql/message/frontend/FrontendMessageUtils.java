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
import io.r2dbc.postgresql.util.Assert;

import java.nio.ByteBuffer;

import static io.netty.util.CharsetUtil.UTF_8;

final class FrontendMessageUtils {

    static final int MESSAGE_OVERHEAD = 1 + 4;

    private static final int LENGTH_PLACEHOLDER = 0;

    private static final byte TERMINAL = 0;

    private FrontendMessageUtils() {
    }

    static ByteBuf writeByte(ByteBuf out, int... values) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(values, "values must not be null");

        for (int value : values) {
            out.writeByte(value);
        }
        return out;
    }

    static ByteBuf writeBytes(ByteBuf out, ByteBuf in) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(in, "in must not be null");

        out.writeBytes(in);
        return out;
    }

    static ByteBuf writeBytes(ByteBuf out, ByteBuffer in) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(in, "in must not be null");

        out.writeBytes(in);
        return out;
    }

    static ByteBuf writeCString(ByteBuf out, ByteBuf in) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(in, "in must not be null");

        out.writeBytes(in, in.readerIndex(), in.readableBytes());
        out.writeByte(TERMINAL);
        return out;
    }

    static ByteBuf writeCStringUTF8(ByteBuf out, CharSequence s) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(s, "s must not be null");

        out.writeCharSequence(s, UTF_8);
        out.writeByte(TERMINAL);
        return out;
    }

    static ByteBuf writeInt(ByteBuf out, int... values) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(values, "values must not be null");

        for (int value : values) {
            out.writeInt(value);
        }
        return out;
    }

    static ByteBuf writeLengthPlaceholder(ByteBuf out) {
        Assert.requireNonNull(out, "out must not be null");

        out.writeInt(LENGTH_PLACEHOLDER);
        return out;
    }

    static ByteBuf writeShort(ByteBuf out, int... values) {
        Assert.requireNonNull(out, "out must not be null");
        Assert.requireNonNull(values, "values must not be null");

        for (int value : values) {
            out.writeShort(value);
        }
        return out;
    }

    static ByteBuf writeSize(ByteBuf out) {
        Assert.requireNonNull(out, "out must not be null");

        return writeSize(out, 1);
    }

    static ByteBuf writeSize(ByteBuf out, int startIndex) {
        Assert.requireNonNull(out, "out must not be null");

        out.setInt(startIndex, out.writerIndex() - startIndex);
        return out;
    }

}
