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

import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for Statement execution modes. Contains the following execution methods:
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Testable
public class StatementBenchmarks {

    private static PostgresqlServerExtension extension = new PostgresqlServerExtension();

    @State(Scope.Benchmark)
    public static class ConnectionHolder {

        final Connection jdbc;

        final io.r2dbc.spi.Connection r2dbc;

        public ConnectionHolder() {

            extension.initialize();
            try {
                jdbc = extension.getDataSource().getConnection();

                Statement statement = jdbc.createStatement();

                try {
                    statement.execute("DROP TABLE IF EXISTS simple_test");
                } catch (SQLException e) {
                }

                statement.execute("CREATE TABLE simple_test (name VARCHAR(255))");
                statement.execute("INSERT INTO simple_test VALUES('foo')");
                statement.execute("INSERT INTO simple_test VALUES('bar')");
                statement.execute("INSERT INTO simple_test VALUES('baz')");

                jdbc.setAutoCommit(false);

                r2dbc = new PostgresqlConnectionFactory(extension.getConnectionConfiguration()).create().block();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Benchmark
    public void simpleJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        Statement statement = connectionHolder.jdbc.createStatement();
        statement.setFetchSize(0);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM simple_test");

        while (resultSet.next()) {
            voodoo.consume(resultSet.getString("name"));
        }

        resultSet.close();
        statement.close();
    }

    @Benchmark
    public void simpleR2dbc(ConnectionHolder connectionHolder, Blackhole voodoo) {

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

    @Benchmark
    public void extendedJdbc(ConnectionHolder connectionHolder, Blackhole voodoo) throws SQLException {

        PreparedStatement statement = connectionHolder.jdbc.prepareStatement("SELECT * FROM simple_test WHERE name = ?");
        statement.setString(1, "plpgsql");
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

        io.r2dbc.spi.Statement statement = connectionHolder.r2dbc.createStatement("SELECT * FROM simple_test WHERE name = $1").bind("$1", "plpgsql");

        String name = Flux.from(statement.execute()).flatMap(it -> it.map((row, rowMetadata) -> row.get("name", String.class))).blockLast();

        voodoo.consume(name);
    }

}
