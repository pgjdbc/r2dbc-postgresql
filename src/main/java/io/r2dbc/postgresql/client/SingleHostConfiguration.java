package io.r2dbc.postgresql.client;

import reactor.util.annotation.Nullable;

public class SingleHostConfiguration {

    @Nullable
    private final String host;

    private final int port;

    @Nullable
    private final String socket;

    public SingleHostConfiguration(@Nullable String host, int port, @Nullable String socket) {
        this.host = host;
        this.port = port;
        this.socket = socket;
    }

    @Nullable
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getRequiredHost() {

        String host = getHost();

        if (host == null || host.isEmpty()) {
            throw new IllegalStateException("Connection is configured for socket connections and not for host usage");
        }

        return host;
    }

    public String getRequiredSocket() {

        String socket = getSocket();

        if (socket == null || socket.isEmpty()) {
            throw new IllegalStateException("Connection is configured to use host and port connections and not for socket usage");
        }

        return socket;
    }

    @Nullable
    public String getSocket() {
        return socket;
    }

    public boolean isUseSocket() {
        return getSocket() != null;
    }

}
