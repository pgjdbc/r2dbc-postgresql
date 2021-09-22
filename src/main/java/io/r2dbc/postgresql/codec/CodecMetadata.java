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

import io.r2dbc.postgresql.message.Format;

/**
 * Metadata for a codec.
 * <p>Codecs implementing this interface expose their supported {@link Format formats} and {@link #getDataTypes() data types}. This metadata can be used for cache warming of the actual codec lookup.
 *
 * @see PostgresTypeIdentifier
 * @see Format
 * @see Codec
 * @since 0.9
 */
public interface CodecMetadata {

    /**
     * Returns the Java {@link Class type} of this codec.
     *
     * @return the Java type
     */
    Class<?> type();

    /**
     * Returns the collection of {@link Format} supported by this codec
     *
     * @return the formats
     */
    default Iterable<Format> getFormats() {
        return Format.all();
    }

    /**
     * Returns the collection of {@link PostgresTypeIdentifier} this codec can handle
     *
     * @return the datatypes
     */
    Iterable<PostgresTypeIdentifier> getDataTypes();

}
