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

package io.r2dbc.postgresql.replication;

import org.jspecify.annotations.Nullable;

/**
 * Information returned on replication slot creation.
 *
 * <p>Returned keys of CREATE_REPLICATION_SLOT:
 * <ol>
 * <li><b>slot_name</b> String {@code =>} the slot name
 * <li><b>consistent_point</b> String {@code =>} LSN at which we became consistent
 * <li><b>snapshot_name</b> String {@code =>} exported snapshot's name (may be {@code null})
 * <li><b>output_plugin</b> String {@code =>} output plugin (may be {@code null})
 * </ol>
 */
public final class ReplicationSlot {

    private final String slotName;

    private final ReplicationType replicationType;

    private final LogSequenceNumber consistentPoint;

    private final @Nullable String snapshotName;

    private final @Nullable String outputPlugin;

    public ReplicationSlot(String slotName, ReplicationType replicationType,
                           LogSequenceNumber consistentPoint, @Nullable String snapshotName, @Nullable String outputPlugin) {
        this.slotName = slotName;
        this.replicationType = replicationType;
        this.consistentPoint = consistentPoint;
        this.snapshotName = snapshotName;
        this.outputPlugin = outputPlugin;
    }

    /**
     * Returns the replication slot name.
     *
     * @return the slot name
     */
    public String getSlotName() {
        return this.slotName;
    }

    /**
     * Replication type of the slot created, {@code PHYSICAL} or {@code LOGICAL}.
     *
     * @return {@link ReplicationType}, {@code PHYSICAL} or {@code LOGICAL}
     */
    public ReplicationType getReplicationType() {
        return this.replicationType;
    }

    /**
     * Returns the {@link LogSequenceNumber LSN} at which we became consistent.
     *
     * @return {@link LogSequenceNumber} at {@code consistent_point}
     */
    public LogSequenceNumber getConsistentPoint() {
        return this.consistentPoint;
    }

    /**
     * Returns the exported snapshot name at the point of replication slot creation.
     *
     * <p>As long as the exporting transaction remains open, other transactions can import its snapshot,
     * and thereby be guaranteed that they see exactly the same view of the database that the first
     * transaction sees.
     *
     * @return exported {@code snapshot_name}
     */
    public @Nullable String getSnapshotName() {
        return this.snapshotName;
    }

    /**
     * Returns the output plugin used on slot creation.
     *
     * @return the output plugin used on slot creation
     */
    public @Nullable String getOutputPlugin() {
        return this.outputPlugin;
    }

}
