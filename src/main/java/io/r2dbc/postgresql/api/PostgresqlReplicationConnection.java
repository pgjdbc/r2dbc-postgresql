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

package io.r2dbc.postgresql.api;

import io.r2dbc.postgresql.replication.ReplicationRequest;
import io.r2dbc.postgresql.replication.ReplicationSlot;
import io.r2dbc.postgresql.replication.ReplicationSlotRequest;
import io.r2dbc.postgresql.replication.ReplicationStream;
import io.r2dbc.spi.Closeable;
import reactor.core.publisher.Mono;

/**
 * A PostgreSQL replication connection.
 */
public interface PostgresqlReplicationConnection extends Closeable {

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Void> close();

    /**
     * Create a replication slot for logical or physical replication.
     *
     * @param request description of the slot to create
     * @return {@link Mono} emitting {@link ReplicationSlot} information once the slot was created
     */
    Mono<ReplicationSlot> createSlot(ReplicationSlotRequest request);

    /**
     * Starts the {@link ReplicationStream} for logical or physical replication.
     * After starting the replication stream this connection becomes unavailable for slot creation and other streams unless the {@link ReplicationStream} is {@link ReplicationStream#close() closed}.
     *
     * @param request description of the replication stream to create
     * @return {@link Mono} emitting {@link ReplicationStream} once the replication was started
     */
    Mono<ReplicationStream> startReplication(ReplicationRequest request);

    /**
     * Returns the {@link PostgresqlConnectionMetadata} for this connection.
     *
     * @return the {@link PostgresqlConnectionMetadata} for this connection
     */
    PostgresqlConnectionMetadata getMetadata();

}
