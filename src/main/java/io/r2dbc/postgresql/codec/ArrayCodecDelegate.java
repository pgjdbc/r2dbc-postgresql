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
import org.jspecify.annotations.Nullable;

/**
 * Interface for codecs that should be used as delegate from {@link ArrayCodec}.
 *
 * @since 0.8.8
 */
interface ArrayCodecDelegate<T> extends CodecMetadata, Decoder<T> {

    /**
     * Encode the {@code value} to be used as parameter to be used as array element.
     *
     * @param value the  {@code value}
     * @return the encoded value
     */
    String encodeToText(T value);

    /**
     * Return the array data type to be used when creating the encoded form of the array.
     *
     * @return the array data type to be used when creating the encoded form of the array.
     */
    PostgresTypeIdentifier getArrayDataType();

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
    @Override
    T decode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends T> type);

}
