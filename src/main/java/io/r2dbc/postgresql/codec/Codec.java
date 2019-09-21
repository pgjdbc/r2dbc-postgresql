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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import reactor.util.annotation.Nullable;

/**
 * Codec to encode and decode values based on Postgres OIDs and {@link Format}.
 * <p>Codecs can decode one or more server-specific data types and represent them as a specific Java {@link Class type}. The type parameter of {@link Codec}
 * indicates the interchange type that is handled by this codec.
 *
 * @param <T> the type that is handled by this codec.
 * @see PostgresqlObjectId
 */
public interface Codec<T> {

    /**
     * Determine whether this {@link Codec} is capable of decoding a value for the given {@code dataType} and {@link Format} and whether it can represent the decoded value as the desired
     * {@link Class type}.
     *
     * @param dataType the dataType to decode
     * @param format   the data type {@link Format}, text or binary
     * @param type     the desired value type
     * @return {@code true} if this codec is able to decode values for tge given {@code dataType} and {@link Format}
     */
    boolean canDecode(int dataType, Format format, Class<?> type);

    /**
     * Determine whether this {@link Codec} is capable of encoding the {@code value}.
     *
     * @param value the parameter value
     * @return {@code true} if this {@link Codec} is able to encode the {@code value}.
     * @see #encodeNull
     */
    boolean canEncode(Object value);

    /**
     * Determine whether this {@link Codec} is capable of encoding a {@code null} value for the given {@link Class} type.
     *
     * @param type the desired value type
     * @return {@code true} if this {@link Codec} is able to encode {@code null} values for the given {@link Class} type.
     * @see #encodeNull
     */
    boolean canEncodeNull(Class<?> type);

    /**
     * Decode the {@link ByteBuf buffer} and return it as the requested {@link Class type}.
     *
     * @param buffer   the data buffer
     * @param dataType the dataType to decode
     * @param format   the data type {@link Format}, text or binary
     * @param type     the desired value type
     * @return the decoded value. Can be {@code null} if the value is {@code null}.
     */
    @Nullable
    T decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends T> type);

    /**
     * Encode the {@code value} to be used as RPC parameter.
     *
     * @param value the {@code null} {@code value}
     * @return the encoded value
     */
    Parameter encode(Object value);

    /**
     * Encode a {@code null} value.
     *
     * @return the encoded {@code null} value
     */
    Parameter encodeNull();

    /**
     * Returns the Java {@link Class type} of this codec.
     *
     * @return the Java type
     */
    Class<?> type();

}
