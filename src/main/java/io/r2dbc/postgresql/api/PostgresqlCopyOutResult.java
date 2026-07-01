/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.api;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.message.Format;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.function.BiFunction;

/**
 * A result obtained by executing a {@code COPY ... TO STDOUT} statement.
 */
public interface PostgresqlCopyOutResult {

    /**
     * Returns a mapping of the rows that are the results of a copy-out operation against a database. May be empty if
     * the query did not return any rows.
     * <p>According to the PostgreSQL documentation, each {@link ByteBuf} will represent a complete row either in
     * binary or textual representation, however it is not clear whether that is just an incidental implementation
     * quirk or a strict guarantee. In the other direction, there is no such requirement, so robust consumers should
     * probably not make any assumptions here.
     * <p>The {@link ByteBuf} instances are only valid to use during the {@code mappingFunction} callback, unless they
     * are retained (via {@link ReferenceCounted#retain()} and later released. When retaining them, care must be taken
     * to ensure that they do eventually get released, as the asynchronous nature of streams can often see messages
     * dropped when the stream is canceled downstream.
     *
     * @param mappingFunction the {@link BiFunction} that maps a {@link ByteBuf} and {@link CopyOutMetadata} to a value
     * @param <T>             the type of the mapped value
     * @return a mapping of the rows that are the results of a copy-out operation against a database
     * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
     * @throws IllegalStateException    if the result was consumed
     */
    <T> Flux<T> map(BiFunction<ByteBuf, CopyOutMetadata, ? extends T> mappingFunction);

    /**
     * Metadata for a copy-out operation.
     */
    interface CopyOutMetadata {
        /**
         * Get the format of each of the columns.
         *
         * @return The format of each of the columns
         */
        Collection<Format> getColumnFormats();

        /**
         * Get the overall format of a response.
         *
         * @return The format.
         */
        Format getOverallFormat();
    }
}
