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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Value object representing a request to create a replication slot.
 * <p>Use {@link #logical()}  to configure a logical replication stream and {@link #physical()} to configure a physical one.
 */
public abstract class ReplicationRequest {

    final ReplicationType replicationType;

    final String slotName;

    final LogSequenceNumber startPosition;

    final Duration statusInterval;

    ReplicationRequest(ReplicationType replicationType, String slotName, LogSequenceNumber startPosition, Duration statusInterval) {
        this.replicationType = Assert.requireNonNull(replicationType, "replicationType must not be null");
        this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null");
        this.startPosition = Assert.requireNonNull(startPosition, "startPosition must not be null");
        this.statusInterval = Assert.requireNonNull(statusInterval, "statusInterval must not be null");
    }

    /**
     * Creates a new builder to configure a logical {@link ReplicationRequest}.
     *
     * @return a new builder to configure a logical {@link ReplicationRequest}.
     */
    public static LogicalReplicationStep1 logical() {
        return new DefaultLogicalReplicationRequestBuilder();
    }

    /**
     * Creates a new builder to configure a physical {@link ReplicationRequest}.
     *
     * @return a new builder to configure a physical {@link ReplicationRequest}.
     */
    public static PhysicalReplicationStep1 physical() {
        return new DefaultPhysicalReplicationRequestBuilder();
    }

    /**
     * Creates a new builder to configure a logical {@link ReplicationRequest} from {@link ReplicationSlot}.
     *
     * @param replicationSlot the replication slot to initialize {@link LogicalReplicationRequestBuilder}.
     * @return a new builder to configure a logical {@link ReplicationRequest} from {@link ReplicationSlot}.
     */
    public static LogicalReplicationRequestBuilder logical(ReplicationSlot replicationSlot) {
        Assert.requireNonNull(replicationSlot, "replicationSlot must not be null");
        return logical().slotName(replicationSlot.getSlotName()).startPosition(replicationSlot.getConsistentPoint());
    }

    /**
     * Renders this request as SQL.
     *
     * @return this request as SQL.
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
     * @return status update interval
     */
    public Duration getStatusInterval() {
        return this.statusInterval;
    }

    static class LogicalReplicationRequest extends ReplicationRequest {

        private final Map<String, Object> slotOptions;

        LogicalReplicationRequest(String slotName, LogSequenceNumber startPosition, Duration statusInterval, Map<String, Object> slotOptions) {
            super(ReplicationType.LOGICAL, slotName, startPosition, statusInterval);
            this.slotOptions = slotOptions;
        }

        @Override
        public String asSQL() {

            String sql = String.format("START_REPLICATION SLOT %s LOGICAL %s", this.slotName, this.startPosition.asString());

            if (this.slotOptions.isEmpty()) {
                return sql;
            }

            StringJoiner joiner = new StringJoiner(", ", " (", ")");
            for (String name : this.slotOptions.keySet()) {
                joiner.add(String.format("\"%s\" '%s'", name, this.slotOptions.get(name)));
            }

            return sql + joiner;
        }

    }

    static class PhysicalReplicationRequest extends ReplicationRequest {

        public PhysicalReplicationRequest(String slotName, LogSequenceNumber startPosition, Duration statusInterval) {
            super(ReplicationType.PHYSICAL, slotName, startPosition, statusInterval);
        }

        @Override
        public String asSQL() {
            return String.format("START_REPLICATION SLOT %s PHYSICAL %s", this.slotName, this.startPosition.asString());
        }

    }

    final static class DefaultLogicalReplicationRequestBuilder implements LogicalReplicationRequestBuilder {

        private String slotName;

        private LogSequenceNumber startPosition;

        private Duration statusInterval = Duration.ofSeconds(10);

        private Map<String, Object> slotOptions = new LinkedHashMap<>();

        @Override
        public LogicalReplicationRequestBuilder slotName(String slotName) {
            this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null and not empty");
            return this;
        }

        @Override
        public LogicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn) {
            this.startPosition = Assert.requireNonNull(lsn, "lsn must not be null");
            return this;
        }

        @Override
        public LogicalReplicationRequestBuilder statusInterval(Duration interval) {
            this.statusInterval = Assert.requireNonNull(interval, "interval must not be null");
            return this;
        }

        @Override
        public LogicalReplicationRequestBuilder slotOption(String option, Object value) {
            Assert.requireNotEmpty(option, "option must not be null and not empty");
            Assert.requireNonNull(value, "value must not be null");
            this.slotOptions.put(option, value);

            return this;
        }

        @Override
        public ReplicationRequest build() {
            return new LogicalReplicationRequest(this.slotName, this.startPosition, this.statusInterval, this.slotOptions);
        }

    }

    final static class DefaultPhysicalReplicationRequestBuilder implements PhysicalReplicationRequestBuilder {

        private String slotName;

        private LogSequenceNumber startPosition;

        private Duration statusInterval = Duration.ofSeconds(10);

        @Override
        public PhysicalReplicationRequestBuilder slotName(String slotName) {
            this.slotName = Assert.requireNotEmpty(slotName, "slotName must not be null and not empty");
            return this;
        }

        @Override
        public PhysicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn) {
            this.startPosition = Assert.requireNonNull(lsn, "lsn must not be null");
            return this;
        }

        @Override
        public PhysicalReplicationRequestBuilder statusInterval(Duration interval) {
            this.statusInterval = Assert.requireNonNull(interval, "interval must not be null");
            return this;
        }

        @Override
        public ReplicationRequest build() {
            return new PhysicalReplicationRequest(this.slotName, this.startPosition, this.statusInterval);
        }

    }

    /**
     * Fluent builder interface to configure the slot name for a logical replication slot.
     */
    public interface LogicalReplicationStep1 extends WithSlotName {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationStep2 slotName(String slotName);

    }

    public interface LogicalReplicationStep2 extends LogicalReplicationStep1, WithStartPosition {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn);

    }

    /**
     * Fluent builder interface to configure a logical replication stream.
     */
    public interface LogicalReplicationRequestBuilder extends LogicalReplicationStep1, LogicalReplicationStep2, WithSlotName, WithStartPosition, WithStatusInterval, WithSlotOption {

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationRequestBuilder slotName(String slotName);

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn);

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationRequestBuilder statusInterval(Duration interval);

        /**
         * {@inheritDoc}
         */
        @Override
        LogicalReplicationRequestBuilder slotOption(String option, Object value);

        /**
         * Returns the logical {@link ReplicationRequest}.
         *
         * @return the logical {@link ReplicationRequest}.
         */
        ReplicationRequest build();

    }

    /**
     * Fluent builder interface to configure the slot name for a physical replication slot.
     */
    public interface PhysicalReplicationStep1 extends WithSlotName {

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalReplicationStep2 slotName(String slotName);

    }

    public interface PhysicalReplicationStep2 extends PhysicalReplicationStep1, WithStartPosition {

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn);

    }

    /**
     * Fluent builder interface to configure a physical replication stream.
     */
    public interface PhysicalReplicationRequestBuilder extends PhysicalReplicationStep1, PhysicalReplicationStep2, WithSlotName, WithStartPosition, WithStatusInterval {

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalReplicationRequestBuilder slotName(String slotName);

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalReplicationRequestBuilder startPosition(LogSequenceNumber lsn);

        /**
         * {@inheritDoc}
         */
        @Override
        PhysicalReplicationRequestBuilder statusInterval(Duration interval);

        /**
         * Returns the physical {@link ReplicationRequest}.
         *
         * @return the physical {@link ReplicationRequest}.
         */
        ReplicationRequest build();

    }

    /**
     * Fluent builder interface fragment to associate the builder with status interval updates.
     */
    public interface WithStatusInterval {

        /**
         * Specifies the number of time between status packets sent back to the server. This allows for easier monitoring of the progress from server. A value of zero disables the periodic status
         * updates completely, although an update will still be sent when requested by the server, to avoid timeout disconnect. The default value is 10 seconds.
         *
         * @param interval positive time
         * @return {@code this} builder.
         * @throws IllegalArgumentException if {@code interval} is {@code null}
         */
        WithStatusInterval statusInterval(Duration interval);

    }

    /**
     * Fluent builder interface fragment to associate the builder with slot options.
     */
    public interface WithSlotOption {

        /**
         * Configure slot option.
         *
         * @param option slot option name
         * @param value  option value
         * @return this instance as a fluent interface
         * @throws IllegalArgumentException if {@code option} or {@code value} is {@code null} or {@code option} is empty
         */
        WithSlotName slotOption(String option, Object value);

    }

    /**
     * Fluent builder interface fragment to associate the builder with the slot name.
     */
    public interface WithSlotName {

        /**
         * Replication slots provide an automated way to ensure that the master does not remove WAL segments until they have been received by all standbys, and that the master does not remove rows
         * which could cause a recovery conflict even when the standby is disconnected.
         *
         * @param slotName not null replication slot already exists on server
         * @return {@code this} builder
         * @throws IllegalArgumentException if {@code slotName} is {@code null} or empty
         */
        WithSlotName slotName(String slotName);

    }

    public interface WithStartPosition {

        /**
         * Specify start position from which backend will start stream changes. If parameter will not specify, streaming starts from restart_lsn. For more details see pg_replication_slots description.
         *
         * @param lsn not null position from which need start replicate changes
         * @return {@code this} builder
         * @throws IllegalArgumentException if {@code slotName} is {@code null}
         */
        WithStartPosition startPosition(LogSequenceNumber lsn);

    }

}
