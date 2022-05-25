/*
 * Copyright 2022 the original author or authors.
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

import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link PostgresqlSqlParser}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Testable
public class PostgresqlSqlParserBenchmarks extends BenchmarkSettings {

    @Benchmark
    public void simpleStatement(Blackhole blackhole) {
        blackhole.consume(PostgresqlSqlParser.parse("SELECT * FROM FOO"));
    }

    @Benchmark
    public void parametrizedStatement(Blackhole blackhole) {
        blackhole.consume(PostgresqlSqlParser.parse("SELECT * FROM FOO WHERE $2 = $1"));
    }

    @Benchmark
    public void createOrReplaceFunction(Blackhole blackhole) {
        blackhole.consume(PostgresqlSqlParser.parse("CREATE OR REPLACE FUNCTION asterisks(n int)\n" +
            "  RETURNS SETOF text\n" +
            "  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE\n" +
            "BEGIN ATOMIC\n" +
            "SELECT repeat('*', g) FROM generate_series (1, n) g; -- <-- Note this semicolon\n" +
            "END;"));
    }

}
