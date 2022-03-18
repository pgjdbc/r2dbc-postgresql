/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Benchmarks for Copy operation. Contains the following execution methods:
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Testable
public class CopyInBenchmarks extends BenchmarkSettings {

    private static PostgresqlServerExtension extension = new PostgresqlServerExtension();

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        @Param({"0", "1", "100", "1000000"})
        int rows;

        final PgConnection jdbc;

        final CopyManager copyManager;

        final PostgresqlConnection r2dbc;

        Path csvFile;

        public ConnectionHolder() {

            extension.initialize();
            try {
                jdbc = extension.getDataSource().getConnection()
                    .unwrap(PgConnection.class);
                copyManager = jdbc.getCopyAPI();
                Statement statement = jdbc.createStatement();

                try {
                    statement.execute("DROP TABLE IF EXISTS simple_test");
                } catch (SQLException e) {
                }

                statement.execute("CREATE TABLE simple_test (name VARCHAR(255), age int)");

                jdbc.setAutoCommit(false);

                r2dbc = new PostgresqlConnectionFactory(extension.getConnectionConfiguration()).create().block();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            csvFile = Files.createTempFile("jmh-input", ".csv");

            try (OutputStream outputStream = new FileOutputStream(csvFile.toFile())) {
                IntStream.range(0, rows)
                    .mapToObj(i -> "some-input" + i + ";" + i + "\n")
                    .forEach(row -> {
                        try {
                            outputStream.write(row.getBytes(StandardCharsets.UTF_8));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            Files.delete(csvFile);
        }

    }

    @Benchmark
    public void copyInR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {
        int bufferSize = 65536; // BufferSize is the same as the one from JDBC's CopyManager
        Flux<ByteBuffer> input = DataBufferUtils.read(connectionHolder.csvFile, DefaultDataBufferFactory.sharedInstance, bufferSize, StandardOpenOption.READ)
            .map(DataBuffer::asByteBuffer);

        Long rowsInserted = connectionHolder.r2dbc.copyIn("COPY simple_test (name, age) FROM STDIN DELIMITER ';'", input)
            .block();

        voodoo.consume(rowsInserted);
    }

    @Benchmark
    public void copyInJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws IOException, SQLException {
        try (InputStream inputStream = new FileInputStream(connectionHolder.csvFile.toFile())) {

            Long rowsInserted = connectionHolder.copyManager.copyIn("COPY simple_test (name, age) FROM STDIN DELIMITER ';'", inputStream);

            voodoo.consume(rowsInserted);
        }
    }

}
