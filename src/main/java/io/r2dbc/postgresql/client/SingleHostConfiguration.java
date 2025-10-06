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

import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.PostgresqlConnectionConfiguration.DEFAULT_PORT;

/**
 * Connection configuration information for connecting to a single Postgres server.
 *
 * @since 1.0
 */
public final class SingleHostConfiguration {

    @Nullable
    private final String host;

    private final int port;

    @Nullable
    private final String socket;

    private SingleHostConfiguration(@Nullable String host, int port, @Nullable String socket) {
        this.host = host;
        this.port = port;
        this.socket = socket;
    }

    @Nullable
    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
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
        return this.socket;
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
    public static final class Builder {

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
         * Configure the port. Defaults to {@code 5432}.
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
            return this.socket;
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
