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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.ExceptionFactory.PostgresqlBadGrammarException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PostgresqlCopyOutResult}.
 */
class PostgresqlCopyOutResultIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void setUp() {
        super.setUp();
        getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        getJdbcOperations().execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value VARCHAR(255))");

    }

    @AfterEach
    void tearDown() {
        super.tearDown();
        getJdbcOperations().execute("DROP TABLE IF EXISTS test");
    }

    private JdbcOperations getJdbcOperations() {
        return SERVER.getJdbcOperations();
    }

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.preparedStatementCacheQueries(2);
    }

    @Test
    void shouldCopyDataOutOfTable() {
        getJdbcOperations().execute("INSERT INTO test (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        String sql = "COPY (SELECT id, value FROM test ORDER BY id) TO STDOUT WITH CSV";

        this.connection.copyOut(sql)
                .flatMap(result -> result.map(this::convertRow))
                .collectSortedList()
                .as(StepVerifier::create)
                .expectNext(asList("1,a\n", "2,b\n", "3,c\n"))
                .verifyComplete();
    }

    @Test
    void shouldCopyDataOutOfEmptyTable() {
        String sql = "COPY (SELECT id, value FROM test ORDER BY id) TO STDOUT WITH CSV";

        this.connection.copyOut(sql)
                .flatMap(result -> result.map(this::convertRow))
                .as(StepVerifier::create)
                .verifyComplete();
    }

    @Test
    void shouldFailOnInvalidStatement() {
        String sql = "COPY invalid command TO STDOUT";

        this.connection.copyOut(sql)
                .flatMap(result -> result.map(this::convertRow))
                .as(StepVerifier::create)
                .verifyError();
    }

    @Test
    void shouldFailOnOrdinarySelectStatement() {
        getJdbcOperations().execute("INSERT INTO test (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        String sql = "SELECT * FROM test";

        this.connection.copyOut(sql)
                .flatMap(result -> result.map(this::convertRow))
                .as(StepVerifier::create)
                .verifyErrorMatches(e -> e.getMessage().contains("copyOut may only be used"));
    }

    private String convertRow(ByteBuf bytes, io.r2dbc.postgresql.api.PostgresqlCopyOutResult.CopyOutMetadata meta) {
        return bytes.toString(StandardCharsets.UTF_8);
    }
}
