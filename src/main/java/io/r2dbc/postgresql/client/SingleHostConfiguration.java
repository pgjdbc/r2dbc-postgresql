package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.PostgresqlConnectionConfiguration.DEFAULT_PORT;

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

    @Override
    public String toString() {
        return "SingleHostConfiguration{" +
            "host='" + this.host + '\'' +
            ", port=" + this.port +
            ", socket='" + this.socket + '\'' +
            '}';
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
     * A builder for {@link SingleHostConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static class Builder {

        @Nullable
        private String host;

        private int port = DEFAULT_PORT;

        @Nullable
        private String socket;

        /**
         * Configure the host.
         *
         * @param host the host
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code host} is {@code null}
         */
        public Builder host(String host) {
            this.host = Assert.requireNonNull(host, "host must not be null");
            return this;
        }

        /**
         * Configure the port.  Defaults to {@code 5432}.
         *
         * @param port the port
         * @return this {@link Builder}
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Configure the unix domain socket to connect to.
         *
         * @param socket the socket path
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code socket} is {@code null}
         */
        public Builder socket(String socket) {
            this.socket = Assert.requireNonNull(socket, "host must not be null");
            return this;
        }

        /**
         * Returns a configured {@link SingleHostConfiguration}.
         *
         * @return a configured {@link SingleHostConfiguration}
         */
        public SingleHostConfiguration build() {
            if (this.host == null && this.socket == null) {
                throw new IllegalArgumentException("host or socket must not be null");
            }
            if (this.host != null && this.socket != null) {
                throw new IllegalArgumentException("Connection must be configured for either host/port or socket usage but not both");
            }

            return new SingleHostConfiguration(this.host, this.port, this.socket);
        }

        @Nullable
        public String getSocket() {
            return socket;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "host='" + this.host + '\'' +
                ", port=" + this.port +
                ", socket='" + this.socket + '\'' +
                '}';
        }
    }


}
