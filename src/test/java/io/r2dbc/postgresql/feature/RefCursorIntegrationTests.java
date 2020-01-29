/*
 * Copyright 2020-2020 the original author or authors.
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

package io.r2dbc.postgresql.feature;

import io.r2dbc.postgresql.AbstractIntegrationTests;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.RefCursor;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgResultSet;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class RefCursorIntegrationTests extends AbstractIntegrationTests {

    @BeforeEach
    void prepare() {

        SERVER.getJdbcOperations().execute("CREATE TABLE IF NOT EXISTS cities (city VARCHAR(255), state VARCHAR(2))");
        SERVER.getJdbcOperations().execute("DELETE FROM cities");

        SERVER.getJdbcOperations().execute("CREATE OR REPLACE FUNCTION show_cities() RETURNS refcursor AS $$\n" +
            "    DECLARE\n" +
            "      ref refcursor;                                                     -- Declare a cursor variable\n" +
            "    BEGIN\n" +
            "      OPEN ref FOR SELECT city, state FROM cities;   -- Open a cursor\n" +
            "      RETURN ref;                                                       -- Return the cursor to the caller\n" +
            "    END;\n" +
            "    $$ LANGUAGE plpgsql;");


        SERVER.getJdbcOperations().execute("CREATE OR REPLACE FUNCTION show_cities_multiple() RETURNS SETOF refcursor AS $$\n" +
            "    DECLARE\n" +
            "      ref1 refcursor;           -- Declare cursor variables\n" +
            "      ref2 refcursor;                             \n" +
            "    BEGIN\n" +
            "      OPEN ref1 FOR SELECT city, state FROM cities WHERE state = 'BW';   -- Open the first cursor\n" +
            "      RETURN NEXT ref1;                                                                              -- Return the cursor to the caller\n" +
            " \n" +
            "      OPEN ref2 FOR SELECT city, state FROM cities WHERE state = 'HE';   -- Open the second cursor\n" +
            "      RETURN NEXT ref2;                                                                              -- Return the cursor to the caller\n" +
            "    END;\n" +
            "    $$ LANGUAGE plpgsql;");

        SERVER.getJdbcOperations().execute("INSERT INTO cities VALUES('Weinheim', 'BW')");
        SERVER.getJdbcOperations().execute("INSERT INTO cities VALUES('Frankfurt', 'HE')");

        connection.setAutoCommit(false).as(StepVerifier::create).verifyComplete();
    }

    @Test
    void fetchCursorWithoutTxShouldFail() {

        connection.setAutoCommit(true).as(StepVerifier::create).verifyComplete();

        connection.createStatement("SELECT show_cities()").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, RefCursor.class)))
            .flatMap(RefCursor::fetch)
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void shouldReturnSingleRefCursorFromJdbc() {

        DataSourceTransactionManager tm = new DataSourceTransactionManager(SERVER.getDataSource());

        TransactionTemplate tt = new TransactionTemplate(tm);

        Map<String, Object> object = tt.execute(transactionStatus -> {
            return SERVER.getJdbcOperations().queryForMap("SELECT show_cities()");
        });

        assertThat(object).containsKey("show_cities");
        assertThat(object.get("show_cities")).isInstanceOf(PgResultSet.class);

        PgResultSet rs = (PgResultSet) object.get("show_cities");
        assertThat(rs.getRefCursor()).isNotEmpty();
    }

    @Test
    void shouldReturnSingleRefCursor() {

        List<String> results = new ArrayList<>();

        connection.createStatement("SELECT show_cities()").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, RefCursor.class)))
            .flatMap(rc -> rc.fetch().flatMapMany(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
                .doOnNext(results::add)
                .then(rc.close()))
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(results).contains("Weinheim", "Frankfurt");
    }

    @Test
    void shouldReturnRefCursor() {

        connection.createStatement("SELECT show_cities()").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0)))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isInstanceOf(RefCursor.class))
            .verifyComplete();
    }

    @Test
    void shouldReturnCursorNameAsString() {

        connection.createStatement("SELECT show_cities()").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class)))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void shouldReturnMultipleRefCursorFromJdbc() {

        DataSourceTransactionManager tm = new DataSourceTransactionManager(SERVER.getDataSource());

        TransactionTemplate tt = new TransactionTemplate(tm);

        List<Map<String, Object>> cursors = tt.execute(transactionStatus -> {
            return SERVER.getJdbcOperations().queryForList("SELECT show_cities_multiple()");
        });

        assertThat(cursors).hasSize(2);
    }

    @Test
    void shouldReturnMultipleRefCursor() {

        List<String> results = new ArrayList<>();
        List<String> cursorNames = new ArrayList<>();

        connection.createStatement("SELECT show_cities_multiple()").execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, RefCursor.class)))
            .flatMap(rc -> {
                cursorNames.add(rc.getCursorName());
                return rc.fetch().flatMapMany(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
                    .doOnNext(results::add)
                    .then(rc.close());
            })
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(results).contains("Weinheim", "Frankfurt");
        assertThat(cursorNames).hasSize(2);
    }

}
