package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.TargetServerType;

import java.util.List;

public class MultipleHostsConfiguration {

    private final int hostRecheckTime;

    private final List<ServerHost> hosts;

    private final boolean loadBalance;

    private final TargetServerType targetServerType;

    public MultipleHostsConfiguration(List<ServerHost> hosts, int hostRecheckTime, boolean loadBalance, TargetServerType targetServerType) {
        this.hosts = hosts;
        this.hostRecheckTime = hostRecheckTime;
        this.loadBalance = loadBalance;
        this.targetServerType = targetServerType;
    }

    public int getHostRecheckTime() {
        return hostRecheckTime;
    }

    public List<ServerHost> getHosts() {
        return hosts;
    }

    public TargetServerType getTargetServerType() {
        return targetServerType;
    }

    public boolean isLoadBalance() {
        return loadBalance;
    }

    public static class ServerHost {

        private final String host;

        private final int port;

        public ServerHost(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}
