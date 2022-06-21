/*
 * Copyright 2019-2020 the original author or authors.
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
 * {@code pgpool} extension that can be added in front of {@link PostgresqlServerExtension}.
 */
public final class PgPool implements AutoCloseable {

    private final GenericContainer<?> container;

    public PgPool(PostgresqlServerExtension server) {
        this.container = new GenericContainer<>("bitnami/pgpool:4.1.0")
            .withExposedPorts(PostgreSQLContainer.POSTGRESQL_PORT)
            .withEnv("PGPOOL_BACKEND_NODES", String.format("0:%s:%s", server.getPostgres().getNetworkAlias(), PostgreSQLContainer.POSTGRESQL_PORT))
            .withEnv("PGPOOL_SR_CHECK_USER", server.getUsername())
            .withEnv("PGPOOL_SR_CHECK_PASSWORD", server.getPassword())
            .withEnv("PGPOOL_ADMIN_USERNAME", server.getUsername())
            .withEnv("PGPOOL_ADMIN_PASSWORD", server.getPassword())
            .withEnv("PGPOOL_POSTGRES_USERNAME", server.getUsername())
            .withEnv("PGPOOL_POSTGRES_PASSWORD", server.getPassword())
            .withEnv("PGPOOL_USERNAME", server.getUsername())
            .withEnv("PGPOOL_PASSWORD", server.getPassword())
            .withEnv("PGPOOL_ENABLE_LDAP", "no")
            .waitingFor(new HostPortWaitStrategy())
            .withNetwork(server.getPostgres().getNetwork());

        this.container.start();
    }

    @Override
    public void close() {
        this.container.stop();
    }

    public String getHost() {
        return this.container.getHost();
    }

    public int getPort() {
        return this.container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

}
