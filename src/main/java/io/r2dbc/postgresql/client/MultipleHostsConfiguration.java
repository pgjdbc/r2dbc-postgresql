package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.TargetServerType;
import io.r2dbc.postgresql.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.r2dbc.postgresql.PostgresqlConnectionConfiguration.DEFAULT_PORT;

public class MultipleHostsConfiguration {

    private final List<ServerHost> hosts;

    private final Duration hostRecheckTime;

    private final boolean loadBalanceHosts;

    private final TargetServerType targetServerType;

    public MultipleHostsConfiguration(List<ServerHost> hosts, Duration hostRecheckTime, boolean loadBalanceHosts, TargetServerType targetServerType) {
        this.hosts = hosts;
        this.hostRecheckTime = hostRecheckTime;
        this.loadBalanceHosts = loadBalanceHosts;
        this.targetServerType = targetServerType;
    }

    public Duration getHostRecheckTime() {
        return hostRecheckTime;
    }

    public List<ServerHost> getHosts() {
        return hosts;
    }

    public TargetServerType getTargetServerType() {
        return targetServerType;
    }

    public boolean isLoadBalanceHosts() {
        return loadBalanceHosts;
    }

    @Override
    public String toString() {
        return "MultipleHostsConfiguration{" +
            "hosts=" + this.hosts +
            ", hostRecheckTime=" + this.hostRecheckTime +
            ", loadBalanceHosts=" + this.loadBalanceHosts +
            ", targetServerType=" + this.targetServerType +
            '}';
    }

    public static class ServerHost {

        private final String host;

        private final int port;

        public ServerHost(String host, int port) {
            this.host = host;
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
            return "ServerHost{" +
                "host='" + this.host + '\'' +
                ", port=" + this.port +
                '}';
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
     * A builder for {@link MultipleHostsConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static class Builder {

        private Duration hostRecheckTime = Duration.ofMillis(10000);

        private List<ServerHost> hosts = new ArrayList<>();

        private boolean loadBalanceHosts = false;

        private TargetServerType targetServerType = TargetServerType.ANY;

        /**
         * Allows opening connections to only servers with required state, the allowed values are any, master, slave, secondary, preferSlave and preferSecondary.
         * The master/secondary distinction is currently done by observing if the server allows writes.
         * The value preferSecondary tries to connect to secondary if any are available, otherwise allows falls back to connecting also to master.
         * Default value is any.
         *
         * @param targetServerType target server type
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code targetServerType} is {@code null}
         */
        public Builder targetServerType(TargetServerType targetServerType) {
            this.targetServerType = Assert.requireNonNull(targetServerType, "targetServerType must not be null");
            return this;
        }

        /**
         * Controls how long in seconds the knowledge about a host state is cached connection factory. The default value is 10000 milliseconds.
         *
         * @param hostRecheckTime host recheck time in milliseconds
         * @return this {@link Builder}
         */
        public Builder hostRecheckTime(Duration hostRecheckTime) {
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
         * Add host with default port to hosts list.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder addHost(String host) {
            Assert.requireNonNull(host, "host must not be null");
            this.hosts.add(new ServerHost(host, DEFAULT_PORT));
            return this;
        }

        /**
         * Add host to hosts list.
         *
         * @param host the host
         * @param port the port
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder addHost(String host, int port) {
            Assert.requireNonNull(host, "host must not be null");
            this.hosts.add(new ServerHost(host, port));
            return this;
        }

        /**
         * Returns a configured {@link MultipleHostsConfiguration}.
         *
         * @return a configured {@link MultipleHostsConfiguration}
         */
        public MultipleHostsConfiguration build() {
            if (this.hosts.isEmpty()) {
                throw new IllegalArgumentException("At least one host should be provided");
            }

            return new MultipleHostsConfiguration(this.hosts, this.hostRecheckTime, this.loadBalanceHosts, this.targetServerType);
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
