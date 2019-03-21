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

package io.r2dbc.postgresql.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Utilities for working with {@link ByteBuffer}s.
 */
public final class ByteBufferUtils {

    private ByteBufferUtils() {
    }

    /**
     * Decode a {@link ByteBuffer} into a {@link String}.
     *
     * @param byteBuffer the {@link ByteBuffer} to decode
     * @return the {@link String} decoded from the {@link ByteBuffer}
     * @throws IllegalArgumentException if {@code byteBuffer} is {@code null}
     */
    public static String decode(ByteBuffer byteBuffer) {
        Assert.requireNonNull(byteBuffer, "byteBuffer must not be null");

        return StandardCharsets.UTF_8.decode(byteBuffer).toString();
    }

    /**
     * Encode a {@link CharSequence} into a {@link ByteBuffer}.
     *
     * @param s the {@link CharSequence} to encode
     * @return the {@link ByteBuffer} with the {@link CharSequence} encoded within it
     * @throws IllegalArgumentException if {@code s} is {@code null}
     */
    public static ByteBuffer encode(CharSequence s) {
        Assert.requireNonNull(s, "s must not be null");

        ByteBuffer buffer = StandardCharsets.UTF_8.encode(s.toString());
        buffer.position(buffer.limit());
        return buffer;
    }

}
