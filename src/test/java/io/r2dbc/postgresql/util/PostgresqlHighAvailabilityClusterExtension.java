package io.r2dbc.postgresql.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.MountableFile;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.testcontainers.utility.MountableFile.forHostPath;

public class PostgresqlHighAvailabilityClusterExtension implements BeforeAllCallback, AfterAllCallback {

    private PostgreSQLContainer<?> primary;

    private HikariDataSource primaryDataSource;

    private PostgreSQLContainer<?> standby;

    private HikariDataSource standbyDataSource;

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (this.standbyDataSource != null) {
            this.standbyDataSource.close();
        }
        if (this.standby != null) {
            this.standby.stop();
        }
        if (this.primaryDataSource != null) {
            this.primaryDataSource.close();
        }
        if (this.primary != null) {
            this.primary.stop();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        Network network = Network.newNetwork();
        this.startPrimary(network);
        this.startStandby(network);
    }

    public PostgreSQLContainer<?> getPrimary() {
        return this.primary;
    }

    public JdbcTemplate getPrimaryJdbc() {
        return new JdbcTemplate(this.primaryDataSource);
    }

    public PostgreSQLContainer<?> getStandby() {
        return standby;
    }

    public JdbcTemplate getStandbyJdbc() {
        return new JdbcTemplate(this.standbyDataSource);
    }

    private static MountableFile getHostPath(String name, int mode) {
        return forHostPath(getResourcePath(name), mode);
    }

    private static Path getResourcePath(String name) {
        URL resource = PostgresqlHighAvailabilityClusterExtension.class.getClassLoader().getResource(name);
        if (resource == null) {
            throw new IllegalStateException("Resource not found: " + name);
        }

        try {
            return Paths.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot convert to path for: " + name, e);
        }
    }

    private void startPrimary(Network network) {
        this.primary = new PostgreSQLContainer<>("postgres:latest")
            .withNetwork(network)
            .withNetworkAliases("postgres-primary")
            .withCopyFileToContainer(getHostPath("setup-primary.sh", 0755), "/docker-entrypoint-initdb.d/setup-primary.sh")
            .withEnv("PG_REP_USER", "replication")
            .withEnv("PG_REP_PASSWORD", "replication_password");
        this.primary.start();
        HikariConfig primaryConfig = new HikariConfig();
        primaryConfig.setJdbcUrl(this.primary.getJdbcUrl());
        primaryConfig.setUsername(this.primary.getUsername());
        primaryConfig.setPassword(this.primary.getPassword());
        this.primaryDataSource = new HikariDataSource(primaryConfig);
    }

    private void startStandby(Network network) {
        this.standby = new PostgreSQLContainer<>("postgres:latest")
            .withNetwork(network)
            .withCopyFileToContainer(getHostPath("setup-standby.sh", 0755), "/setup-standby.sh")
            .withCommand("/setup-standby.sh")
            .withEnv("PG_REP_USER", "replication")
            .withEnv("PG_REP_PASSWORD", "replication_password")
            .withEnv("PG_MASTER_HOST", "postgres-primary")
            .withEnv("PG_MASTER_PORT", "5432");
        this.standby.setWaitStrategy(new LogMessageWaitStrategy()
            .withRegEx(".*database system is ready to accept read only connections.*\\s")
            .withTimes(1)
            .withStartupTimeout(Duration.of(60L, ChronoUnit.SECONDS)));
        this.standby.start();
        HikariConfig standbyConfig = new HikariConfig();
        standbyConfig.setJdbcUrl(this.standby.getJdbcUrl());
        standbyConfig.setUsername(this.standby.getUsername());
        standbyConfig.setPassword(this.standby.getPassword());
        this.standbyDataSource = new HikariDataSource(standbyConfig);
    }
}
