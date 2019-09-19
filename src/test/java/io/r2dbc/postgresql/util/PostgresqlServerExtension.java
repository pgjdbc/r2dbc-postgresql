/*
 * Copyright 2017-2019 the original author or authors.
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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.util.annotation.Nullable;

import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.testcontainers.utility.MountableFile.forHostPath;

public final class PostgresqlServerExtension implements BeforeAllCallback, AfterAllCallback {

    private volatile PostgreSQLContainer<?> containerInstance = null;

    private final ResourceLoader resourceLoader = new DefaultResourceLoader();

    private final Supplier<PostgreSQLContainer<?>> container = () -> {
        if (this.containerInstance != null) {
            return this.containerInstance;
        }
        return this.containerInstance = container();
    };

    private final DatabaseContainer postgres = new TestContainer(this.container.get());

    private final boolean useTestContainer = this.postgres instanceof TestContainer;

    private final AtomicInteger nestingCounter = new AtomicInteger(0);

    private HikariDataSource dataSource;

    private JdbcOperations jdbcOperations;

    public PostgresqlServerExtension() {
    }

    @Override
    public void afterAll(ExtensionContext context) {
        int nesting = nestingCounter.decrementAndGet();
        if (nesting == 0) {
            if (this.dataSource != null) {
                this.dataSource.close();
            }
            if (this.useTestContainer) {
                this.container.get().stop();
            }
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        int nesting = nestingCounter.incrementAndGet();
        if (nesting == 1) {
            if (this.useTestContainer) {
                this.container.get().start();
            }

            this.dataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .url(String.format("jdbc:postgresql://%s:%d/%s", getHost(), getPort(), getDatabase()))
                .username(getUsername())
                .password(getPassword())
                .build();

            this.dataSource.setMaximumPoolSize(1);

            this.jdbcOperations = new JdbcTemplate(this.dataSource);
        }
    }

    public String getClientCrt() {
        return Paths.get("client.crt").toAbsolutePath().toString();
    }

    public String getClientKey() {
        return Paths.get("client.key").toAbsolutePath().toString();
    }

    public String getDatabase() {
        return this.postgres.getDatabase();
    }

    @Nullable
    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    public String getHost() {
        return this.postgres.getHost();
    }

    public int getPort() {
        return this.postgres.getPort();
    }

    public String getServerCrt() {
        return Paths.get("server.crt").toAbsolutePath().toString();
    }

    public String getServerKey() {
        return Paths.get("server.key").toAbsolutePath().toString();
    }

    public String getUsername() {
        return this.postgres.getUsername();
    }

    public String getPassword() {
        return this.postgres.getPassword();
    }

    private static Path createTempFile(String name, String... lines) throws IOException {
        Path tempFile = Files.createTempFile(null, name);
        tempFile.toFile().deleteOnExit();
        try (FileWriter w = new FileWriter(tempFile.toFile())) {
            for (String line : lines) {
                w.write(line);
            }
        }
        return tempFile;
    }

    private <T extends PostgreSQLContainer<T>> T container() {
        T container = new PostgreSQLContainer<T>("postgres:11.1")
            .withCopyFileToContainer(forHostPath("server.crt", 0600), "/var/server.crt")
            .withCopyFileToContainer(forHostPath("server.key", 0600), "/var/server.key")
            .withCopyFileToContainer(forHostPath("client.crt", 0600), "/var/client.crt")
            .withCopyFileToContainer(forHostPath("pg_hba.conf", 0600), "/var/pg_hba.conf")
            .withCopyFileToContainer(forHostPath("setup.sh", 0755), "/var/setup.sh")
            .withCopyFileToContainer(forHostPath("test-db-init-script.sql", 0755), "/docker-entrypoint-initdb.d/test-db-init-script.sql")
//            .withInitScript("test-db-init-script.sql")
            .withCommand("postgres",
                "-c", "ssl=on",
                "-c", "ssl_key_file=/var/server.key",
                "-c", "ssl_cert_file=/var/server.crt",
                "-c", "ssl_ca_file=/var/client.crt",
                "-c", "hba_file=/var/pg_hba.conf");

//        container.setWaitStrategy(new DockerHealthcheckWaitStrategy().withStartupTimeout(Duration.ofSeconds(15)));

        container.setWaitStrategy(new LogMessageWaitStrategy()
            .withRegEx(".*database system is ready to accept connections.*")
            .withTimes(2)
            .withStartupTimeout(Duration.ofSeconds(60)));

        return container
            ;
    }

    /**
     * Interface to be implemented by database providers (provided database, test container).
     */
    interface DatabaseContainer {

        String getHost();

        int getPort();

        String getDatabase();

        String getUsername();

        String getPassword();
    }

    /**
     * Externally provided Postgres instance.
     */
    static class External implements DatabaseContainer {

        public static final External INSTANCE = new External();

        @Override
        public String getHost() {
            return "localhost";
        }

        @Override
        public int getPort() {
            return 5432;
        }

        @Override
        public String getDatabase() {
            return "postgres";
        }

        @Override
        public String getUsername() {
            return "postgres";
        }

        @Override
        public String getPassword() {
            return "postgres";
        }

        /**
         * Returns whether this container is available.
         *
         * @return
         */
        @SuppressWarnings("try")
        boolean isAvailable() {

            try (Socket ignored = new Socket(getHost(), getPort())) {

                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    /**
     * {@link DatabaseContainer} provided by {@link JdbcDatabaseContainer}.
     */
    static class TestContainer implements DatabaseContainer {

        private final JdbcDatabaseContainer<?> container;

        TestContainer(JdbcDatabaseContainer<?> container) {
            this.container = container;
        }

        @Override
        public String getHost() {
            return this.container.getContainerIpAddress();
        }

        @Override
        public int getPort() {
            return this.container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
        }

        @Override
        public String getDatabase() {
            return this.container.getDatabaseName();
        }

        @Override
        public String getUsername() {
            return this.container.getUsername();
        }

        @Override
        public String getPassword() {
            return this.container.getPassword();
        }
    }
}
