/*
 * Copyright 2021 the original author or authors.
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Integration tests for cached statement invalidation.
 */
class CachedStatementInvalidationIntegrationTests extends AbstractIntegrationTests {

    @AfterAll
    static void afterAll() {
        System.gc();
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void test(Fixture fixture) throws Exception {

        PostgresqlConnectionFactory connectionFactory = getConnectionFactory(builder -> {

            builder.preparedStatementCacheQueries(fixture.preparedStatementQueryCaching).compatibilityMode(fixture.compatibilityMode);
        });

        PostgresqlConnection connection = connectionFactory.create().block();

        JdbcTemplate jdbcOperations = (JdbcTemplate) SERVER.getJdbcOperations();

        jdbcOperations.execute("DROP TABLE IF EXISTS table_to_be_changed;");
        jdbcOperations.execute("CREATE TABLE table_to_be_changed (firstname varchar(50))");
        jdbcOperations.execute("INSERT INTO table_to_be_changed VALUES('foo')");

        runPreparedQuery(connection, fixture);

        // cause statement invalidation
        updateTable(jdbcOperations);

        // should detect invalid statement and recover
        issueCachedQueryAfterTableChange(connection, fixture);

        // verify that subsequent queries run correctly
        verifySubsequentQueries(connection, fixture);

        connection.close().as(StepVerifier::create).verifyComplete();
    }

    private void runPreparedQuery(PostgresqlConnection connection, Fixture fixture) {
        runQuery(connection, fixture);
    }

    private void updateTable(JdbcTemplate jdbcOperations) {
        jdbcOperations.execute("ALTER TABLE table_to_be_changed ALTER COLUMN firstname TYPE varchar(100)");
    }

    private void issueCachedQueryAfterTableChange(PostgresqlConnection connection, Fixture fixture) {
        runQuery(connection, fixture);
    }

    private void verifySubsequentQueries(PostgresqlConnection connection, Fixture fixture) {
        connection.createStatement("SELECT firstname FROM table_to_be_changed")
            .execute()
            .concatMap(it -> it.map((r, md) -> r.get(0)))
            .as(StepVerifier::create)
            .expectNext("foo")
            .verifyComplete();

        runQuery(connection, fixture);
    }

    private void runQuery(PostgresqlConnection connection, Fixture fixture) {
        connection.createStatement("SELECT firstname FROM table_to_be_changed WHERE $1 = $1")
            .bind("$1", 1)
            .fetchSize(fixture.fetchSize).execute()
            .concatMap(it -> it.map((r, md) -> r.get(0)))
            .as(StepVerifier::create)
            .expectNext("foo")
            .verifyComplete();
    }

    static Stream<Fixture> fixtures() {

        List<Fixture> fixtures = new ArrayList<>();

        // disabled, indefinite, single-statement cache

        for (int fetchSize = 0; fetchSize < 10; fetchSize += 10) {
            for (int statementCacheSize = -1; statementCacheSize < 2; statementCacheSize++) {
                fixtures.add(new Fixture(true, fetchSize, statementCacheSize));
                fixtures.add(new Fixture(false, fetchSize, statementCacheSize));
            }
        }

        return fixtures.stream();
    }

    static class Fixture {

        final boolean compatibilityMode;

        final int fetchSize;

        final int preparedStatementQueryCaching;

        public Fixture(boolean compatibilityMode, int fetchSize, int preparedStatementQueryCaching) {
            this.compatibilityMode = compatibilityMode;
            this.fetchSize = fetchSize;
            this.preparedStatementQueryCaching = preparedStatementQueryCaching;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [compatibilityMode=").append(this.compatibilityMode);
            sb.append(", fetchSize=").append(this.fetchSize);
            sb.append(", preparedStatementQueryCaching=").append(this.preparedStatementQueryCaching);
            sb.append(']');
            return sb.toString();
        }

    }

}
