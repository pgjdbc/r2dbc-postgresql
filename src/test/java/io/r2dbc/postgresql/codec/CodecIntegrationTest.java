/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.PostgresqlResult;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;

public class CodecIntegrationTest {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    private final PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
        .database(SERVER.getDatabase())
        .host(SERVER.getHost())
        .port(SERVER.getPort())
        .password(SERVER.getPassword())
        .username(SERVER.getUsername())
        .build();

    private final PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(this.configuration);

    @Test
    void testBigDecimal() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (big_decimal_value) VALUES($1)").
                bind("$1", new BigDecimal("1000.0"))
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT big_decimal_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("big_decimal_value", BigDecimal.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(new BigDecimal("1000.0"))
            .verifyComplete();
    }

    @Test
    void testBoolean() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (boolean_value) VALUES($1)").
                bind("$1", true)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT boolean_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("boolean_value", Boolean.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testByte() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (byte_value) VALUES($1)").
                bind("$1", (byte) 10)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT byte_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("byte_value", Byte.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext((byte) 10)
            .verifyComplete();
    }

    @Test
    void testCharacter() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (character_value) VALUES($1)").
                bind("$1", 'a')
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT character_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("character_value", Character.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext('a')
            .verifyComplete();
    }

    @Test
    void testDate() {
        Date date = new Date();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (date_value) VALUES($1)").
                bind("$1", date)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT date_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("date_value", Date.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(date)
            .verifyComplete();
    }

    @Test
    void testDouble() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (double_value) VALUES($1)").
                bind("$1", 100.0)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT double_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("double_value", Double.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(100.0)
            .verifyComplete();
    }

    @Test
    void testEnum() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (enum_value) VALUES($1)").
                bind("$1", Color.RED)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT enum_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("enum_value", Color.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(Color.RED)
            .verifyComplete();
    }

    @Test
    void testFloat() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (float_value) VALUES($1)").
                bind("$1", 100.0f)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT float_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("float_value", Float.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(100.0f)
            .verifyComplete();
    }

    @Test
    void testInetAddress() throws UnknownHostException {
        this.connectionFactory.create()
            .flatMap(c -> {
                try {
                    return c.createStatement("INSERT INTO codec (inet_address_value) VALUES($1)").
                        bind("$1", InetAddress.getByName("127.0.0.1"))
                        .execute()
                        .flatMap(PostgresqlResult::getRowsUpdated)
                        .single();
                } catch (UnknownHostException e) {
                    return Mono.error(e);
                }
            })
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT inet_address_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("inet_address_value", InetAddress.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(InetAddress.getByName("127.0.0.1"))
            .verifyComplete();
    }

    @Test
    void testInstant() {
        Instant instant = Instant.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (instant_value) VALUES($1)").
                bind("$1", instant)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT instant_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("instant_value", Instant.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(instant)
            .verifyComplete();
    }

    @Test
    void testInteger() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (integer_value) VALUES($1)").
                bind("$1", 100)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT integer_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("integer_value", Integer.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(100)
            .verifyComplete();
    }

    @Test
    void testLocalDate() {
        LocalDate localDate = LocalDate.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (local_date_value) VALUES($1)").
                bind("$1", localDate)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT local_date_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("local_date_value", LocalDate.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(localDate)
            .verifyComplete();
    }

    @Test
    void testLocalDateTime() {
        LocalDateTime localDateTime = LocalDateTime.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (local_date_time_value) VALUES($1)").
                bind("$1", localDateTime)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT local_date_time_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("local_date_time_value", LocalDateTime.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(localDateTime)
            .verifyComplete();
    }

    @Test
    void testLocalTime() {
        LocalTime localTime = LocalTime.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (local_time_value) VALUES($1)").
                bind("$1", localTime)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT local_time_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("local_time_value", LocalTime.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(localTime)
            .verifyComplete();
    }

    @Test
    void testLong() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (long_value) VALUES($1)").
                bind("$1", 100L)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT long_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("long_value", Long.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(100L)
            .verifyComplete();
    }

    @Test
    void testOffsetDateTime() {
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (offset_date_time_value) VALUES($1)").
                bind("$1", offsetDateTime)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT offset_date_time_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("offset_date_time_value", OffsetDateTime.class))
                    .map(odt -> OffsetDateTime.ofInstant(odt.toInstant(), ZoneId.systemDefault()))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(offsetDateTime)
            .verifyComplete();
    }

    @Test
    void testShort() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (short_value) VALUES($1)").
                bind("$1", (short) 100)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT short_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("short_value", Short.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext((short) 100)
            .verifyComplete();
    }

    @Test
    void testString() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (string_value) VALUES($1)").
                bind("$1", "foo")
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT string_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("string_value", String.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext("foo")
            .verifyComplete();
    }

    @Test
    void testUri() {
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (uri_value) VALUES($1)").
                bind("$1", URI.create("http://example.com"))
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT uri_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("uri_value", URI.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(URI.create("http://example.com"))
            .verifyComplete();
    }

    @Test
    void testUrl() throws MalformedURLException {
        this.connectionFactory.create()
            .flatMap(c -> {
                try {
                    return c.createStatement("INSERT INTO codec (url_value) VALUES($1)").
                        bind("$1", new URL("http://example.com"))
                        .execute()
                        .flatMap(PostgresqlResult::getRowsUpdated)
                        .single();
                } catch (MalformedURLException e) {
                    return Mono.error(e);
                }
            })
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT url_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("url_value", URL.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(new URL("http://example.com"))
            .verifyComplete();
    }

    @Test
    void testUuid() throws MalformedURLException {
        UUID uuid = UUID.randomUUID();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (uuid_value) VALUES($1)").
                bind("$1", uuid)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT uuid_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("uuid_value", UUID.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(uuid)
            .verifyComplete();
    }

    @Test
    void testZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (zoned_date_time_value) VALUES($1)").
                bind("$1", zonedDateTime)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT zoned_date_time_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("zoned_date_time_value", ZonedDateTime.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(zonedDateTime)
            .verifyComplete();
    }

    @Test
    void testZoneId() {
        ZoneId zoneId = ZoneId.systemDefault();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("INSERT INTO codec (zone_id_value) VALUES($1)").
                bind("$1", zoneId)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .single())
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
        this.connectionFactory.create()
            .flatMap(c -> c.createStatement("SELECT zone_id_value FROM codec LIMIT 1")
                .execute()
                .single()
                .flatMap(result -> result
                    .map((row, meta) -> row.get("zone_id_value", ZoneId.class))
                    .single()))
            .as(StepVerifier::create)
            .expectNext(zoneId)
            .verifyComplete();
    }

    @BeforeEach
    void createTable() {
        SERVER.getJdbcOperations().execute("CREATE TABLE codec ( " + //
            "  big_decimal_value NUMERIC " + //
            ", boolean_value BOOL " + //
            ", byte_value INT2 " + //
            ", character_value VARCHAR(1) " + //
            ", date_value TIMESTAMP " + //
            ", double_value FLOAT8 " + //
            ", enum_value VARCHAR(16) " + //
            ", float_value FLOAT4 " + //
            ", inet_address_value VARCHAR(128) " + //
            ", instant_value TIMESTAMP " + //
            ", integer_value INT4 " + //
            ", local_date_value DATE " + //
            ", local_date_time_value TIMESTAMP" + //
            ", local_time_value TIME" + //
            ", long_value INT8" + //
            ", offset_date_time_value TIMESTAMP WITH TIME ZONE" + //
            ", short_value INT2 " + //
            ", string_value VARCHAR(128) " + //
            ", uri_value VARCHAR(128) " + //
            ", url_value VARCHAR(128) " + //
            ", uuid_value UUID " + //
            ", zoned_date_time_value TIMESTAMP WITH TIME ZONE" + //
            ", zone_id_value VARCHAR(32)" + //
            ")");
    }

    @AfterEach
    void dropTable() {
        SERVER.getJdbcOperations().execute("DROP TABLE codec");
    }


    public enum Color {
        RED
    }
}
