/*
 * Copyright 2022 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.MultiHostConnectionStrategy;
import io.r2dbc.postgresql.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.r2dbc.postgresql.PostgresqlConnectionConfiguration.DEFAULT_PORT;

/**
 * Connection configuration information for connecting to a cluster of Postgres servers.
 *
 * @since 1.0
 */
public final class MultiHostConfiguration {

    private final List<ServerHost> hosts;

    private final Duration hostRecheckTime;

    private final boolean loadBalanceHosts;

    private final MultiHostConnectionStrategy.TargetServerType targetServerType;

    private MultiHostConfiguration(List<ServerHost> hosts, Duration hostRecheckTime, boolean loadBalanceHosts, MultiHostConnectionStrategy.TargetServerType targetServerType) {
        this.hosts = hosts;
        this.hostRecheckTime = hostRecheckTime;
        this.loadBalanceHosts = loadBalanceHosts;
        this.targetServerType = targetServerType;
    }

    public Duration getHostRecheckTime() {
        return this.hostRecheckTime;
    }

    public List<ServerHost> getHosts() {
        return this.hosts;
    }

    public MultiHostConnectionStrategy.TargetServerType getTargetServerType() {
        return this.targetServerType;
    }

    public boolean isLoadBalanceHosts() {
        return this.loadBalanceHosts;
    }

    @Override
    public String toString() {
        return "MultiHostConfiguration{" +
            "hosts=" + this.hosts +
            ", hostRecheckTime=" + this.hostRecheckTime +
            ", loadBalanceHosts=" + this.loadBalanceHosts +
            ", targetServerType=" + this.targetServerType +
            '}';
    }

    public final static class ServerHost {

        private final String host;

        private final int port;

        public ServerHost(String host, int port) {
            this.host = Assert.requireNotEmpty(host, "host must not be empty");
            this.port = port;
        }

        public String getHost() {
            return this.host;
        }

        public int getPort() {
            return this.port;
        }

        @Override
        public String toString() {
            return this.host + ":" + this.port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ServerHost)) {
                return false;
            }
            ServerHost that = (ServerHost) o;
            return this.port == that.port && Objects.equals(this.host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.host, this.port);
        }

    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link MultiHostConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private Duration hostRecheckTime = Duration.ofSeconds(10);

        private final List<ServerHost> hosts = new ArrayList<>();

        private boolean loadBalanceHosts = false;

        private MultiHostConnectionStrategy.TargetServerType targetServerType = MultiHostConnectionStrategy.TargetServerType.ANY;

        /**
         * Allows opening connections to only servers with required state.
         * The primary/secondary distinction is currently done by observing if the server allows writes.
         * The value {@link MultiHostConnectionStrategy.TargetServerType#PREFER_SECONDARY} tries to connect to a secondary if any are available, otherwise allows falls back to a primary.
         * Default value is {@link MultiHostConnectionStrategy.TargetServerType#ANY}.
         *
         * @param targetServerType target server type
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code targetServerType} is {@code null}
         */
        public Builder targetServerType(MultiHostConnectionStrategy.TargetServerType targetServerType) {
            this.targetServerType = Assert.requireNonNull(targetServerType, "targetServerType must not be null");
            return this;
        }

        /**
         * Controls how long in seconds the knowledge about a host state is cached connection factory. The default value is {@code 10 seconds}.
         *
         * @param hostRecheckTime host recheck time in milliseconds
         * @return this {@link Builder}
         */
        public Builder hostRecheckTime(Duration hostRecheckTime) {
            Assert.isTrue(hostRecheckTime != null && !hostRecheckTime.isNegative(), "Host recheck time must not be null and not negative");
            this.hostRecheckTime = hostRecheckTime;
            return this;
        }

        /**
         * In default mode (disabled) hosts are connected in the given order. If enabled hosts are chosen randomly from the set of suitable candidates.
         *
         * @param loadBalanceHosts is load balance mode enabled
         * @return this {@link Builder}
         */
        public Builder loadBalanceHosts(boolean loadBalanceHosts) {
            this.loadBalanceHosts = loadBalanceHosts;
            return this;
        }

        /**
         * Add host with default port to the hosts list.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder addHost(String host) {
            return addHost(host, DEFAULT_PORT);
        }

        /**
         * Add host to the hosts list.
         *
         * @param host the host
         * @param port the port
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder addHost(String host, int port) {
            Assert.requireNotEmpty(host, "host must not be null and not empty");
            Assert.isTrue(port >= 0 && port < 65535, "port must be in a valid range (0-65534)");
            this.hosts.add(new ServerHost(host, port));
            return this;
        }

        /**
         * Returns a configured {@link MultiHostConfiguration}.
         *
         * @return a configured {@link MultiHostConfiguration}
         */
        public MultiHostConfiguration build() {

            if (this.hosts.isEmpty()) {
                throw new IllegalArgumentException("At least one host must be provided");
            }

            return new MultiHostConfiguration(this.hosts, this.hostRecheckTime, this.loadBalanceHosts, this.targetServerType);
        }

        @Override
        public String toString() {
            return "Builder{" +
                "hostRecheckTime=" + this.hostRecheckTime +
                ", hosts=" + this.hosts +
                ", loadBalanceHosts=" + this.loadBalanceHosts +
                ", targetServerType=" + this.targetServerType +
                '}';
        }

    }

}
