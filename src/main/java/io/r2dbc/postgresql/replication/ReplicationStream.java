/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.postgresql.replication;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.api.PostgresqlReplicationConnection;
import io.r2dbc.spi.Closeable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Postgresql replication stream. Once established, the stream occupies the {@link PostgresqlReplicationConnection} until this stream is {@link #close() closed}. This stream can be consumed by
 * applying a {@link Function mapping function} using {@link #map(Function)}.
 */
public interface ReplicationStream extends Closeable {

    /**
     * Stop replication changes from server and free resources. After that connection can be reused. Also after closing the current stream this object cannot be used anymore. Subscribers will see a
     * completion signal if they are still subscribed.
     *
     * @return a {@link Mono} that termination is complete
     */
    @Override
    Mono<Void> close();

    /**
     * @return {@code true} if replication stream was already closed, otherwise return {@code false}
     */
    boolean isClosed();

    /**
     * Returns a mapping of the replication stream which is an unbounded stream.
     * <p>The {@link ByteBuf data buffer} is released after applying the {@link Function mapping function}.
     * <p>Unsubscribing from the stream will cancel consumption leaving protocol frames on the transport buffer. {@link #close() Close the} {@code ReplicationStream} object to terminate the
     *
     * @param mappingFunction the {@link Function} that maps a {@link ByteBuf} to a value.
     * @param <T>             the type of the mapped value
     * @return a mapping of the {@link ByteBuf data buffers} that are the results of the replication stream
     * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
     */
    <T> Flux<T> map(Function<ByteBuf, ? extends T> mappingFunction);

    /**
     * Returns the last received LSN position.
     *
     * @return LSN position that was received with the last read via {@link #map(Function)}.
     */
    LogSequenceNumber getLastReceiveLSN();

    /**
     * Returns the last flushed lsn send in update message to backend.
     *
     * @return location of the last WAL flushed to disk in the standby
     */
    LogSequenceNumber getLastFlushedLSN();

    /**
     * Returns the last applied lsn send in update message to backed.
     *
     * @return location of the last WAL applied in the standby.
     */
    LogSequenceNumber getLastAppliedLSN();

    /**
     * Sets the flushed LSN. The parameter will be send to backend on next update status iteration. Flushed
     * LSN position help backend define which wal can be recycle.
     *
     * @param flushed not null location of the last WAL flushed to disk in the standby.
     */
    void setFlushedLSN(LogSequenceNumber flushed);

    /**
     * Parameter used only physical replication and define which lsn already was apply on standby.
     * Feedback will send to backend on next update status iteration.
     *
     * @param applied not null location of the last WAL applied in the standby.
     */
    void setAppliedLSN(LogSequenceNumber applied);

}
