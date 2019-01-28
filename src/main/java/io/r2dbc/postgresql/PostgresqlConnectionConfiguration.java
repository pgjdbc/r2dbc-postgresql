/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;


import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

/**
 * Connection configuration information for connecting to a PostgreSQL database.
 */
public final class PostgresqlConnectionConfiguration {

    /**
     * Default PostgreSQL port.
     */
    public static final int DEFAULT_PORT = 5432;

    private final String applicationName;

    private final String database;

    private final String host;

    private final String password;

    private final int port;

    private final String schema;

    private final String username;

    private PostgresqlConnectionConfiguration(String applicationName, @Nullable String database, String host, String password, int port, @Nullable String schema, String username) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.database = database;
        this.host = Assert.requireNonNull(host, "host must not be null");
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.port = port;
        this.schema = schema;
        this.username = Assert.requireNonNull(username, "username must not be null");
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "PostgresConnectionConfiguration{" +
            "applicationName='" + this.applicationName + '\'' +
            ", database='" + this.database + '\'' +
            ", host='" + this.host + '\'' +
            ", password='" + this.password + '\'' +
            ", port=" + this.port +
            ", schema='" + this.schema + '\'' +
            ", username='" + this.username + '\'' +
            '}';
    }

    String getApplicationName() {
        return this.applicationName;
    }

    @Nullable
    String getDatabase() {
        return this.database;
    }

    String getHost() {
        return this.host;
    }

    String getPassword() {
        return this.password;
    }

    int getPort() {
        return this.port;
    }

    @Nullable
    String getSchema() {
        return this.schema;
    }

    String getUsername() {
        return this.username;
    }

    /**
     * A builder for {@link PostgresqlConnectionConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private String applicationName = "postgresql-r2dbc";

        private String database;

        private String host;

        private String password;

        private int port = DEFAULT_PORT;

        private String schema;

        private String username;

        private Builder() {
        }

        /**
         * Configure the application name.  Defaults to {@code postgresql-r2dbc}.
         *
         * @param applicationName the application name
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code applicationName} is {@code null}
         */
        public Builder applicationName(String applicationName) {
            this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
            return this;
        }

        /**
         * Returns a configured {@link PostgresqlConnectionConfiguration}.
         *
         * @return a configured {@link PostgresqlConnectionConfiguration}
         */
        public PostgresqlConnectionConfiguration build() {
            return new PostgresqlConnectionConfiguration(this.applicationName, this.database, this.host, this.password, this.port, this.schema, this.username);
        }

        /**
         * Configure the database.
         *
         * @param database the database
         * @return this {@link Builder}
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

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
         * Configure the password.
         *
         * @param password the password
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code password} is {@code null}
         */
        public Builder password(String password) {
            this.password = Assert.requireNonNull(password, "password must not be null");
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
         * Configure the schema.
         *
         * @param schema the schema
         * @return this {@link Builder}
         */
        public Builder schema(@Nullable String schema) {
            this.schema = schema;
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "applicationName='" + this.applicationName + '\'' +
                ", database='" + this.database + '\'' +
                ", host='" + this.host + '\'' +
                ", password='" + this.password + '\'' +
                ", port=" + this.port +
                ", schema='" + this.schema + '\'' +
                ", username='" + this.username + '\'' +
                '}';
        }

        /**
         * Configure the username.
         *
         * @param username the username
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code username} is {@code null}
         */
        public Builder username(String username) {
            this.username = Assert.requireNonNull(username, "username must not be null");
            return this;
        }

    }

}
