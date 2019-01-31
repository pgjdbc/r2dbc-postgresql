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
import org.testcontainers.utility.MountableFile;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public final class PostgresqlServerExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Path clientCrt;

    private static final Path clientKey;

    private static final Path initContainerConfig;

    private static final Path pgHba;

    private static final Path serverCrt;

    private static final Path serverKey;

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

    static {
        try {
            Path keysDir = Files.createTempDirectory("keys");
            keysDir.toFile().deleteOnExit();
            Tuple2<Path, Path> server = createPair(keysDir, "server", "r2dbc-postgresql-test-server");
            Tuple2<Path, Path> client = createPair(keysDir, "client", "test-ssl-with-cert");
            serverCrt = server.getT1();
            serverKey = server.getT2();
            clientCrt = client.getT1();
            clientKey = client.getT2();
            initContainerConfig = createTempFile("init-container-config.sh",
                "touch /tmp/server.key\n",
                "chown postgres:postgres /tmp/server.key\n",
                "chmod 0600 /tmp/server.key\n",
                "cat /var/server.key > /tmp/server.key\n",
                "echo \"/tmp/server.key initialized\"\n",
                "touch /tmp/pg_hba.conf\n",
                "chmod 0600 /tmp/pg_hba.conf\n",
                "chown postgres:postgres /tmp/pg_hba.conf\n",
                "cat /var/pg_hba.conf > /tmp/pg_hba.conf\n",
                "echo \"/tmp/pg_hba initialized\"\n",
                "cat /var/server.crt\n");
            pgHba = createTempFile("pg_hba.conf",
                "hostnossl         all       test                    all     md5\n",
                "hostnossl         all       test-scram              all     scram-sha-256\n",
                "hostssl           all       test-ssl                all     password\n",
                "hostssl           all       test-ssl-with-cert      all     cert\n");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public PostgresqlServerExtension() {
    }


    public String getClientCrt() {
        return PostgresqlServerExtension.clientCrt.toString();
    }

    public String getClientKey() {
        return PostgresqlServerExtension.clientKey.toString();
    }

    public String getServerCrt() {
        return PostgresqlServerExtension.serverCrt.toString();
    }

    public String getServerKey() {
        return PostgresqlServerExtension.serverKey.toString();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        int nesting = nestingCounter.decrementAndGet();
        if (nesting == 0) {
            this.dataSource.close();
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

    public String getUsername() {
        return this.postgres.getUsername();
    }

    public String getPassword() {
        return this.postgres.getPassword();
    }

    private static Tuple2<Path, Path> createPair(Path dir, String name, String cn) throws IOException, InterruptedException {
        new ProcessBuilder("docker", "run",
            "-v", dir.toAbsolutePath() + ":/out",
            "--rm",
            "--entrypoint", "openssl",
            "frapsoft/openssl",
            "req", "-newkey", "rsa:2048", "-nodes",
            "-keyout", String.format("/out/%s.key", name),
            "-out", String.format("/out/%s.crt", name),
            "-x509",
            "-days", "365",
            "-subj", String.format("/CN=%s", cn))
            .start()
            .waitFor();
        Path cert = dir.resolve(String.format("%s.crt", name));
        Path key = dir.resolve(String.format("%s.key", name));
        cert.toFile().deleteOnExit();
        key.toFile().deleteOnExit();
        return Tuples.of(cert, key);
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
        return new PostgreSQLContainer<T>("postgres:11.1") {

            @Override
            protected void configure() {
                super.configure();
                setCommand(
                    "postgres",
                    "-c", "ssl=on",
                    "-c", "ssl_key_file=/tmp/server.key",
                    "-c", "ssl_cert_file=/var/server.crt",
                    "-c", "ssl_ca_file=/var/client.crt",
                    "-c", "hba_file=/tmp/pg_hba.conf"
                );
            }
        }
            .withInitScript("test-db-init-script.sql")
            .withCopyFileToContainer(MountableFile.forHostPath(serverCrt, 0755), "/var/server.crt")
            .withCopyFileToContainer(MountableFile.forHostPath(serverKey, 0755), "/var/server.key")
            .withCopyFileToContainer(MountableFile.forHostPath(clientCrt, 0755), "/var/client.crt")
            .withCopyFileToContainer(MountableFile.forHostPath(pgHba, 0755), "/var/pg_hba.conf")
            .withCopyFileToContainer(MountableFile.forHostPath(initContainerConfig, 0755), "/docker-entrypoint-initdb.d/init-container-config.sh");
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
