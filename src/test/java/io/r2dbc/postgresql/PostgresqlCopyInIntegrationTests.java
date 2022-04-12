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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.ExceptionFactory.PostgresqlBadGrammarException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PostgresqlCopyIn}.
 */
class PostgresqlCopyInIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void setUp() {
        super.setUp();
        getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        getJdbcOperations().execute("CREATE TABLE test (id SERIAL PRIMARY KEY, val VARCHAR(255), timestamp TIMESTAMP)");
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
    void shouldCopyDataIntoTable() {
        String sql = "COPY test (val) FROM STDIN";

        Flux<ByteBuf> data = Flux.just(
            byteBuf("d\n"),
            byteBuf("d\n"),
            byteBuf("e\n")
        );

        this.connection.copyIn(sql, data)
            .as(StepVerifier::create)
            .expectNext(3L)
            .verifyComplete();

        // Verify the connection is no longer in COPY-IN mode and verify data is copied into the table.
        verifyItemsInserted(asList("d", "d", "e"));
    }

    @Test
    void shouldHandleErrorOnFailureInInput() {
        String sql = "COPY test (val) FROM STDIN";

        Flux<ByteBuf> data = Flux.just(
                byteBuf("d\n")
            )
            .concatWith(Mono.error(new RuntimeException("Failed during input generation")));

        this.connection.copyIn(sql, data)
            .as(StepVerifier::create)
            .expectError(RuntimeException.class)
            .verify();

        verifyItemsInserted(emptyList());
    }

    @Test
    void shouldCopyNothingEmptyFlux() {
        String sql = "COPY test (val) FROM STDIN";

        Flux<ByteBuf> data = Flux.empty();

        this.connection.copyIn(sql, data)
            .as(StepVerifier::create)
            .expectNext(0L)
            .verifyComplete();
    }

    @Test
    void shouldHandleErrorOnValidNonCopyInQuery() {
        String sql = "SELECT 1";

        Flux<ByteBuf> input = Flux.just(byteBuf("something,something-invalid\n"));

        this.connection.copyIn(sql, input)
            .as(StepVerifier::create)
            .consumeErrorWith(e -> assertThat(e)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Copy from stdin query expected, sql='SELECT 1', message=CommandComplete{command=SELECT, rowId=null, rows=1}")
            )
            .verify();
    }

    @Test
    void shouldHandleErrors() {
        String sql = "COPY test (val) FROM STDIN";

        int characterCountVarcharType = 256;
        Flux<ByteBuf> input = Flux.just(String.join("", Collections.nCopies(characterCountVarcharType, "a")))
            .map(this::byteBuf);

        verifyCopyInFailed(sql, input, "value too long for type character varying(255)");
    }

    @Test
    void shouldFailOnInvalidStatement() {
        String sql = "COPY invalid command";

        Flux<ByteBuf> data = Flux.just(byteBuf("something,something-invalid\n"));

        verifyCopyInFailed(sql, data, "syntax error at or near \"command\"");
    }

    @Test
    void shouldFailOnInvalidDataType() {
        String sql = "COPY test (val, timestamp) FROM STDIN WITH DELIMITER ','";

        Flux<ByteBuf> data = Flux.just(byteBuf("something,something-invalid\n"));

        verifyCopyInFailed(sql, data, "invalid input syntax for type timestamp: \"something-invalid\"");
    }

    private void verifyCopyInFailed(String sql, Flux<ByteBuf> data, String message) {
        this.connection.copyIn(sql, data)
            .as(StepVerifier::create)
            .consumeErrorWith(e -> assertThat(e)
                .isInstanceOf(PostgresqlBadGrammarException.class)
                .hasMessage(message)
            )
            .verify();
    }

    private void verifyItemsInserted(List<Object> t) {
        this.connection.createStatement("SELECT val FROM test")
            .execute()
            .flatMap(res -> res.map(row -> row.get(0)))
            .collectSortedList()
            .as(StepVerifier::create)
            .expectNext(t)
            .verifyComplete();
    }

    private ByteBuf byteBuf(String str) {
        return Unpooled.wrappedBuffer(str.getBytes());
    }

}
