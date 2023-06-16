/*
 * Copyright 2023 the original author or authors.
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

import io.r2dbc.postgresql.api.PostgresqlResult;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract test class hosting test cases for temporal values using/exceeding Postgres limits.
 * Executed typically in text and binary mode through subclasses.
 */
abstract class AbstractTemporalCodecIntegrationTests extends AbstractIntegrationTests {

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.timeZone(TimeZone.getTimeZone("UTC"));
    }

    void prepare(String sqlType) {

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test (value %s)", sqlType));
    }

    @Test
    void shouldConsiderLocalDateMinMax() {

        prepare("date");

        insert(LocalDate.MAX);
        expectValue(LocalDate.MAX);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('5874897-12-31')");

        expectValue(LocalDate.of(5874897, 12, 31));

        insert(LocalDate.MIN);
        expectValue(LocalDate.MIN);

        insert(LocalDate.parse("0000-12-31"));
        expectValue(LocalDate.parse("0000-12-31"));
    }

    @Test
    void shouldConsiderLocalTimeMinMax() {

        prepare("time");

        insert(LocalTime.MAX);
        expectValue(LocalTime.MAX);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('24:00:00')");

        expectValue(LocalTime.MAX);

        insert(LocalTime.MIN);
        expectValue(LocalTime.MIN);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('00:00:00')");

        expectValue(LocalTime.MIN);
    }

    @Test
    void shouldConsiderOffsetTimeMinMax() {

        prepare("timetz");

        OffsetTime offsetTime = LocalTime.MAX.atOffset(ZoneOffset.ofHours(12));
        insert(offsetTime);
        expectValue(OffsetTime.MAX);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('24:00:00+0')");

        expectValue(OffsetTime.MAX);

        insert(LocalTime.MIN.atOffset(ZoneOffset.ofHours(12)));
        expectValue(LocalTime.MIN.atOffset(ZoneOffset.ofHours(12)));

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('00:00:00+0')");

        expectValue(LocalTime.MIN.atOffset(ZoneOffset.ofHours(0)));
    }

    @Test
    void shouldConsiderLocalDateTimeMinMax() {

        prepare("timestamp");

        insert(LocalDateTime.MAX);
        expectValue(LocalDateTime.MAX);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('infinity')");

        expectValue(LocalDateTime.MAX);

        insert(LocalDateTime.MIN);
        expectValue(LocalDateTime.MIN);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('-infinity')");

        expectValue(LocalDateTime.MIN);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('01.01.2001 24:00:00')");

        expectValue(LocalDate.of(2001, 01, 02).atTime(LocalTime.MIN));
    }

    @Test
    void shouldConsiderOffsetDateTimeMinMax() {

        prepare("timestamptz");

        insert(OffsetDateTime.MAX);
        expectValue(OffsetDateTime.MAX);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('infinity')");

        expectValue(OffsetDateTime.MAX);

        insert(OffsetDateTime.MIN);
        expectValue(OffsetDateTime.MIN);

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('-infinity')");

        expectValue(OffsetDateTime.MIN);

        // due to our time settings above, TZ UTC, we need to normalize values into the selected timezone
        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('01.01.2001 24:00:00+0')");

        expectValue(LocalDate.of(2001, 01, 02).atTime(LocalTime.MIN).atOffset(ZoneOffset.ofHours(0)));

        insert(OffsetDateTime.parse("0000-12-31T01:01:00+01:00"));
        expectValue(OffsetDateTime.parse("0000-12-31T00:01:00+00:00"));
    }

    @Test
    void shouldConsiderInstantMinMax() {

        prepare("timestamptz");

        insert(Instant.MAX);
        expectValue(OffsetDateTime.MAX.toInstant());

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('infinity')");

        expectValue(OffsetDateTime.MAX.toInstant());

        insert(Instant.MIN);
        expectValue(OffsetDateTime.MIN.toInstant());

        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('-infinity')");

        expectValue(OffsetDateTime.MIN.toInstant());

        // due to our time settings above, TZ UTC, we need to normalize values into the selected timezone
        SERVER.getJdbcOperations().update("DELETE FROM test");
        SERVER.getJdbcOperations().update("INSERT INTO test VALUES ('01.01.2001 24:00:00+0')");

        expectValue(LocalDate.of(2001, 01, 02).atTime(LocalTime.MIN).atOffset(ZoneOffset.ofHours(0)).toInstant());

        insert(Instant.parse("0000-12-31T01:01:00Z"));
        expectValue(Instant.parse("0000-12-31T01:01:00Z"));

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        insert(now);
        expectValue(now);
    }

    private void insert(Object value) {

        SERVER.getJdbcOperations().update("DELETE FROM test");

        connection.createStatement("INSERT INTO test VALUES($1)")
            .bind("$1", value)
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();
    }

    private void expectValue(Object expected) {

        connection.createStatement("SELECT value FROM test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, expected.getClass())))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> {
                assertThat(actual).isEqualTo(expected);
            })
            .verifyComplete();
    }
}
