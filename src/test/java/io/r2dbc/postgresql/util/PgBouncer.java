package io.r2dbc.postgresql.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class PgBouncer implements AutoCloseable {

    private final GenericContainer<?> container;

    public PgBouncer(PostgresqlServerExtension server, String poolMode) {
        this.container = new GenericContainer<>("edoburu/pgbouncer")
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
