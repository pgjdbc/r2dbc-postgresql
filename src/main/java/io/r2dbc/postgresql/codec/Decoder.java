/*
 * Copyright 2021 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.Format;
import reactor.util.annotation.Nullable;

/**
 * Decoder for a specific {@link PostgresTypeIdentifier} and {@link Class type}.
 *
 * @param <T> the type that is handled by this decoder.
 * @since 0.9
 */
interface Decoder<T> {

    /**
     * Decode the {@link ByteBuf buffer} and return it as the requested {@link Class type}.
     *
     * @param buffer   the data buffer
     * @param dataType the Postgres OID to encode
     * @param format   the data type {@link Format}, text or binary
     * @param type     the desired value type
     * @return the decoded value. Can be {@code null} if the value is {@code null}.
     */
    @Nullable
    T decode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends T> type);

}
