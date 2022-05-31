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

import io.r2dbc.postgresql.util.Assert;

/**
 * Value object representing a request to create a replication slot.
 * <p>Use {@link #logical()}  to configure a logical replication slot and {@link #physical()} to configure a physical one.
 */
public abstract class ReplicationSlotRequest {

    private final ReplicationType replicationType;

    private final String slotName;

    private final boolean temporary;

    ReplicationSlotRequest(ReplicationType replicationType, String slotName, boolean temporary) {
        this.replicationType = Assert.requireNonNull(replicationType, "replicationType must not be null");
        this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null");
        this.temporary = temporary;
    }

    /**
     * Create a new builder to configure a logical {@link ReplicationSlotRequest}.
     *
     * @return a new builder to configure a logical {@link ReplicationSlotRequest}.
     */
    public static LogicalSlotRequestBuilderStep1 logical() {
        return new DefaultLogicalSlotRequestBuilder();
    }

    /**
     * Create a new builder to configure a physical {@link ReplicationSlotRequest}.
     *
     * @return a new builder to configure a physical {@link ReplicationSlotRequest}.
     */
    public static PhysicalSlotRequestBuilderStep1 physical() {
        return new DefaultPhysicalSlotRequestBuilder();
    }

    /**
     * Renders this request as SQL.
     *
     * @return this request as SQL
     */
    public abstract String asSQL();

    /**
     * Returns the replication type of the slot, {@code PHYSICAL} or {@code LOGICAL}.
     *
     * @return {@link ReplicationType}, {@code PHYSICAL} or {@code LOGICAL}
     */
    public ReplicationType getReplicationType() {
        return this.replicationType;
    }

    /**
     * Returns the replication slot name.
     *
     * @return the slot name
     */
    String getSlotName() {
        return this.slotName;
    }

    /**
     * Returns the slot is temporary.
     *
     * @return {@code true} if the slot should be temporary
     */
    boolean isTemporary() {
        return this.temporary;
    }

    /**
     * Slot creation request for logical replication.
     */
    static class LogicalReplicationSlotRequest extends ReplicationSlotRequest {

        private final String outputPlugin;

        public LogicalReplicationSlotRequest(String slotName, boolean temporaryOption, String outputPlugin) {
            super(ReplicationType.LOGICAL, slotName, temporaryOption);
            this.outputPlugin = outputPlugin;
        }

        @Override
        public String asSQL() {
            return String.format(
                "CREATE_REPLICATION_SLOT %s %s LOGICAL %s",
                getSlotName(),
                isTemporary() ? "TEMPORARY" : "",
                this.outputPlugin
            );
        }

        @Override
        public String toString() {
            return "LogicalReplicationSlotRequest{" +
                "slotName='" + getSlotName() + '\'' +
                ", outputPlugin='" + this.outputPlugin + '\'' +
                ", temporaryOption=" + isTemporary() +
                '}';
        }

    }

    /**
     * Slot creation request for physical replication.
     */
    static class PhysicalReplicationSlotRequest extends ReplicationSlotRequest {

        public PhysicalReplicationSlotRequest(String slotName, boolean temporaryOption) {
            super(ReplicationType.PHYSICAL, slotName, temporaryOption);
        }

        @Override
        public String asSQL() {
            return String.format(
                "CREATE_REPLICATION_SLOT %s %s PHYSICAL",
                getSlotName(),
                isTemporary() ? "TEMPORARY" : ""
            );
        }

        @Override
        public String toString() {
            return "PhysicalReplicationSlotRequest{" +
                "slotName='" + getSlotName() + '\'' +
                ", temporaryOption=" + isTemporary() +
                '}';
        }

    }

    final static class DefaultLogicalSlotRequestBuilder implements LogicalSlotRequestBuilder {

        private String slotName;

        private String outputPlugin;

        private boolean temporary;

        @Override
        public LogicalSlotRequestBuilder slotName(String slotName) {
            this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null and not empty");
            return this;
        }

        @Override
        public LogicalSlotRequestBuilder outputPlugin(String outputPlugin) {
            this.outputPlugin = Assert.requireNotEmpty(outputPlugin, "outputPlugin must not be null and not empty");
            return this;
        }

        @Override
        public LogicalSlotRequestBuilder temporary() {
            this.temporary = true;
            return this;
        }

        @Override
        public ReplicationSlotRequest build() {
            return new LogicalReplicationSlotRequest(this.slotName, this.temporary, this.outputPlugin);
        }

    }

    final static class DefaultPhysicalSlotRequestBuilder implements PhysicalSlotRequestBuilder {

        private String slotName;

        private boolean temporary;

        @Override
        public DefaultPhysicalSlotRequestBuilder slotName(String slotName) {
            this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null and not empty");
            return this;
        }

        @Override
        public DefaultPhysicalSlotRequestBuilder temporary() {
            this.temporary = true;
            return this;
        }

        @Override
        public ReplicationSlotRequest build() {
            return new PhysicalReplicationSlotRequest(this.slotName, this.temporary);
        }

    }

    /**
     * Fluent builder interface to configure the slot name for a logical replication slot.
     */
    public interface LogicalSlotRequestBuilderStep1 extends WithSlotName {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalSlotRequestBuilderStep2 slotName(String slotName);

    }

    public interface LogicalSlotRequestBuilderStep2 extends WithOutputPlugin, WithSlotName {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalSlotRequestBuilder outputPlugin(String outputPlugin);

    }

    /**
     * Fluent builder interface to configure a logical replication slot.
     */
    public interface LogicalSlotRequestBuilder extends LogicalSlotRequestBuilderStep1, LogicalSlotRequestBuilderStep2, WithTemporary {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalSlotRequestBuilder slotName(String slotName);

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalSlotRequestBuilder temporary();

        /**
         * Builds the logical {@link ReplicationSlotRequest}.
         *
         * @return the logical {@link ReplicationSlotRequest}.
         */
        ReplicationSlotRequest build();

    }

    /**
     * Fluent builder interface to configure the slot name for a physical replication slot.
     */
    public interface PhysicalSlotRequestBuilderStep1 extends WithSlotName {

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalSlotRequestBuilder slotName(String slotName);

    }

    /**
     * Fluent builder interface to configure a physical replication slot.
     */
    public interface PhysicalSlotRequestBuilder extends PhysicalSlotRequestBuilderStep1, WithTemporary {

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalSlotRequestBuilder slotName(String slotName);

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalSlotRequestBuilder temporary();

        /**
         * Builds the physical {@link ReplicationSlotRequest}.
         *
         * @return the physical {@link ReplicationSlotRequest}.
         */
        ReplicationSlotRequest build();

    }

    /**
     * Fluent builder interface fragment to associate the builder with the temporary option.
     */
    public interface WithTemporary {

        /**
         * <p>Temporary slots are not saved to disk and are automatically dropped on error or when
         * the session has finished.
         *
         * <p>This feature is only supported by PostgreSQL versions &gt;= 10.</p>
         *
         * @return {@code this} builder
         */
        WithTemporary temporary();

    }

    /**
     * Fluent builder interface fragment to associate the builder with a slot name.
     */
    public interface WithSlotName {

        /**
         * Replication slots provide an automated way to ensure that the primary does not remove WAL segments until they have been received by all standbys, and that the primary does not remove rows
         * which could cause a recovery conflict even when the standby is disconnected.
         *
         * @param slotName replication slot name for create, must not be {@code null} or empty
         * @return {@code this} builder
         * @throws IllegalArgumentException if {@code slotName} is {@code null} or empty
         */
        WithSlotName slotName(String slotName);

    }

    /**
     * Fluent builder interface fragment to associate the builder with an output plugin.
     */
    public interface WithOutputPlugin {

        /**
         * <p>Output plugin that should be use for decode physical represent WAL to some logical form. Output plugin should be installed on server(exists in shared_preload_libraries).
         *
         * <p>Package postgresql-contrib provides sample output plugin <b>test_decoding</b> that can be use for test logical replication api.
         *
         * @param outputPlugin name of the output plugin used for logical decoding, must not be {@code null} or empty
         * @return {@code this} builder
         * @throws IllegalArgumentException if {@code outputPlugin} is {@code null} or empty
         */
        WithOutputPlugin outputPlugin(String outputPlugin);

    }

}
