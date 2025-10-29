/*
 * Copyright 2025 the original author or authors.
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
 * Marker interface to indicate a codec expresses preference for certain data types and formats.
 *
 * @since 1.1.1
 */
interface PreferredCodec {

    /**
     * Determine whether the codec expresses preference for the given data type, format and type.
     * <p>Codecs that return {@code true} should also return {@code true} from {@link Codec#canDecode(int, Format, Class)} for the same arguments to honor the API contract between the two methods.
     *
     * @param dataType the Postgres OID to decode
     * @param format   the data type {@link Format}, text or binary
     * @param type     the desired value type
     * @return {@code true} if this codec wants to be preferred to decode values for tge given {@code dataType} and {@link Format}
     */
    boolean isPreferred(int dataType, Format format, Class<?> type);

}
