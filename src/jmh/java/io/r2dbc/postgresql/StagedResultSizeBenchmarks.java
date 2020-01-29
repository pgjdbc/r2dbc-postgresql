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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for Statement execution modes across various result sizes.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Testable
public class StagedResultSizeBenchmarks extends BenchmarkSettings {

    private static PostgresqlServerExtension extension = new PostgresqlServerExtension();

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        final Connection jdbc;

        final io.r2dbc.spi.Connection r2dbc;

        @Param({"1", "10", "100", "200"})
        int resultSize;

        public ConnectionHolder() {

            try {

                extension.initialize();
                jdbc = extension.getDataSource().getConnection();

                r2dbc = new PostgresqlConnectionFactory(extension.getConnectionConfiguration()).create().block();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Setup
        public void setup() {
            try {
                Statement statement = jdbc.createStatement();

                statement.execute("DROP TABLE IF EXISTS result_sizes");
                statement.execute("CREATE TABLE result_sizes (id int, name VARCHAR(255))");

                for (int i = 0; i < resultSize; i++) {
                    statement.execute(String.format("INSERT INTO result_sizes VALUES(%d, '%s')", i, UUID.randomUUID().toString()));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Benchmark
    public void simpleJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_sizes");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void simpleR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void extendedJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        connectionHolder.jdbc.setAutoCommit(false);
        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM result_sizes where id != ?");
        statement.setInt(1, 0);
        statement.setFetchSize(50);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void extendedR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM result_sizes WHERE name != $1").bind("$1", "foo");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

}
