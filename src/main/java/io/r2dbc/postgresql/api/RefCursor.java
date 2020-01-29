/*
 * Copyright 2020-2020 the original author or authors.
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

import io.r2dbc.spi.Closeable;
import reactor.core.publisher.Mono;

/**
 * A ref cursor value object. Cursor objects can be attached to a {@link PostgresqlConnection} which allows interaction with the cursor object by {@link #fetch() fetching the cursor} and
 * {@link #close() closing} it.
 * Cursor objects are stateful resources. Obtaining a cursor typically keeps the cursor open on the database server. The cursor object can get invalidated if the transaction is closed. Cursor
 * objects should be {@link #close() closed} if they are no longer required.
 * Detached cursors that were instantiated without a connection may throw {@link UnsupportedOperationException} when attempting to fetch or close the cursor.
 */
public interface RefCursor extends Closeable {

    /**
     * Return the ref cursor name (portal name).
     *
     * @return the ref cursor name (portal name).
     */
    String getCursorName();

    /**
     * Fetch the contents of the cursor using {@code FETCH ALL IN}.
     *
     * @return the {@link PostgresqlResult} associated with the ref cursor.
     */
    Mono<PostgresqlResult> fetch();

    /**
     * Close the cursor.
     *
     * @return a {@link Mono} terminating with success the cursor was closed.
     */
    @Override
    Mono<Void> close();

}
