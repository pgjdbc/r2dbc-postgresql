/*
 * Copyright 2019-2021 the original author or authors.
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

package io.r2dbc.postgresql.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * {@code pgbouncer} extension that can be added in front of {@link PostgresqlServerExtension}.
 */
public final class PgBouncer implements AutoCloseable {

    private final GenericContainer<?> container;

    public PgBouncer(PostgresqlServerExtension server, String poolMode) {
        this.container = new GenericContainer<>("edoburu/pgbouncer:1.9.0")
            .withExposedPorts(PostgreSQLContainer.POSTGRESQL_PORT)
            .withEnv("POOL_MODE", poolMode)
            .withEnv("SERVER_RESET_QUERY_ALWAYS", "1")
            .withEnv("DB_USER", server.getUsername())
            .withEnv("DB_PASSWORD", server.getPassword())
            .withEnv("DB_HOST", server.getPostgres().getNetworkAlias())
            .withEnv("DB_PORT", String.valueOf(PostgreSQLContainer.POSTGRESQL_PORT))
            .withEnv("DB_NAME", server.getDatabase())
            .waitingFor(new HostPortWaitStrategy())
            .withNetwork(server.getPostgres().getNetwork());

        this.container.start();
    }

    @Override
    public void close() {
        this.container.stop();
    }

    public String getHost() {
        return this.container.getContainerIpAddress();
    }

    public int getPort() {
        return this.container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

}
