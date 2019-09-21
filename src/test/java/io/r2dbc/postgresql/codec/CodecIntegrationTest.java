/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

final class CodecIntegrationTest {

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
    void bigDecimal() {
        testCodec(BigDecimal.class, new BigDecimal("1000.0"), "NUMERIC");
    }

    @Test
    void booleanPrimitive() {
        testCodec(Boolean.class, true, "BOOL");
    }

    @Test
    void bytePrimitive() {
        testCodec(Byte.class, (byte) 10, "INT2");
    }

    @Test
    void charPrimitive() {
        testCodec(Character.class, 'a', "BPCHAR(1)");
        testCodec(Character.class, 'a', "VARCHAR(1)");
    }

    @Test
    void clob() {
        testCodec(Clob.class,
            new Clob() {

                @Override
                public Publisher<Void> discard() {
                    return Mono.empty();
                }

                @Override
                public Publisher<CharSequence> stream() {
                    return Mono.just("test-value");
                }
            },
            (actual, expected) -> Flux.zip(
                Flux.from(actual.stream()).reduce(new StringBuilder(), StringBuilder::append).map(StringBuilder::toString),
                Flux.from(expected.stream()).reduce(new StringBuilder(), StringBuilder::append).map(StringBuilder::toString)
            )
                .as(StepVerifier::create)
                .assertNext(t -> assertThat(t.getT1()).isEqualToIgnoringWhitespace(t.getT2()))
                .verifyComplete()
            , "TEXT");
    }

    @Test
    void blob() {
        final byte[] bytes = {1, 2, 3, 4};
        testCodec(Blob.class,
            new Blob() {

                @Override
                public Publisher<Void> discard() {
                    return Mono.empty();
                }

                @Override
                public Publisher<ByteBuffer> stream() {
                    return Mono.just(ByteBuffer.wrap(bytes));
                }
            },
            (actual, expected) -> Flux.zip(
                Flux.from(actual.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes).concatWith(Mono.from(actual.discard()).then(Mono.empty())),
                Flux.from(expected.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes).concatWith(Mono.from(expected.discard()).then(Mono.empty()))
            )
                .as(StepVerifier::create)
                .assertNext(t -> assertThat(t.getT1()).isEqualTo(t.getT2()))
                .verifyComplete()
            , "BYTEA");
    }

    @Test
    void date() {
        testCodec(Date.class, new Date(), "TIMESTAMP");
    }

    @Test
    void doublePrimitive() {
        testCodec(Double.class, 100.0, "FLOAT8");
    }

    @Test
    void floatPrimitive() {
        testCodec(Float.class, 100.0F, "FLOAT4");
    }

    @Test
    void inetAddress() throws UnknownHostException {
        testCodec(InetAddress.class, InetAddress.getLocalHost(), "BPCHAR(128)");
        testCodec(InetAddress.class, InetAddress.getLocalHost(), "VARCHAR(128)");
    }

    @Test
    void instant() {
        testCodec(Instant.class, Instant.now(), "TIMESTAMP");
    }

    @Test
    void intArray() {
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "INT4[]");
    }

    @Test
    void intPrimitive() {
        testCodec(Integer.class, 100, "INT4");
    }

    @Test
    void intTwoDimensionalArray() {
        testCodec(Integer[][].class, new Integer[][]{{100, 200}, {300, null}}, "INT4[][]");
    }

    @Test
    void localDate() {
        testCodec(LocalDate.class, LocalDate.now(), "DATE");
    }

    @Test
    void localDateTime() {
        testCodec(LocalDateTime.class, LocalDateTime.now(), "TIMESTAMP");
    }

    @Test
    void localTime() {
        testCodec(LocalTime.class, LocalTime.now(), "TIME");
    }

    @Test
    void longArray() {
        testCodec(Long[].class, new Long[]{100L, 200L, 300L}, "INT8[]");
    }

    @Test
    void longPrimitive() {
        testCodec(Long.class, 100L, "INT8");
    }

    @Test
    void longTwoDimensionalArray() {
        testCodec(Long[][].class, new Long[][]{{100L, 200L}, {300L, null}}, "INT8[][]");
    }

    @Test
    void offsetDateTime() {
        testCodec(OffsetDateTime.class, OffsetDateTime.now(), (actual, expected) -> assertThat(actual.isEqual(expected)).isTrue(), "TIMESTAMP WITH TIME ZONE");
    }

    @Test
    void shortArray() {
        testCodec(Short[].class, new Short[]{100, 200, 300}, "INT2[]");
    }

    @Test
    void shortPrimitive() {
        testCodec(Short.class, (short) 100, "INT2");
    }

    @Test
    void shortTwoDimensionalArray() {
        testCodec(Short[][].class, new Short[][]{{100, 200}, {300, null}}, "INT2[][]");
    }

    @Test
    void string() {
        testCodec(String.class, "test-value", (actual, expected) -> assertThat(actual).isEqualToIgnoringWhitespace(expected), "BPCHAR(32)");
        testCodec(String.class, "test-value", "VARCHAR(32)");
    }

    @Test
    void stringArray() {
        testCodec(String[].class, new String[]{"test-value1", "test-value2", "test-value3"}, "BPCHAR[]");
        testCodec(String[].class, new String[]{"test-value1", "test-value2", "test-value3"}, "VARCHAR[]");
    }

    @Test
    void stringArrayValueEscaping() {
        testCodec(String[].class, new String[]{"NULL", null, "R \"2\" DBC", "АБ"}, "BPCHAR[]");
        testCodec(String[].class, new String[]{"NULL", null, "R \"2\" DBC", "АБ"}, "VARCHAR[]");
    }

    @Test
    void stringTwoDimensionalArray() {
        testCodec(String[][].class, new String[][]{{"test-value1"}, {"test-value2"}}, "BPCHAR[]");
        testCodec(String[][].class, new String[][]{{"test-value1"}, {"test-value2"}}, "VARCHAR[]");
    }

    @Test
    void uri() {
        testCodec(URI.class, URI.create("https://localhost"), "BPCHAR(128)");
        testCodec(URI.class, URI.create("https://localhost"), "VARCHAR(128)");
    }

    @Test
    void url() throws MalformedURLException {
        testCodec(URL.class, new URL("https://localhost"), "BPCHAR(128)");
        testCodec(URL.class, new URL("https://localhost"), "VARCHAR(128)");
    }

    @Test
    void uuid() {
        testCodec(UUID.class, UUID.randomUUID(), "UUID");
    }

    @Test
    void zoneId() {
        testCodec(ZoneId.class, ZoneId.systemDefault(), "BPCHAR(32)");
        testCodec(ZoneId.class, ZoneId.systemDefault(), "VARCHAR(32)");
    }

    @Test
    void zonedDateTime() {
        testCodec(ZonedDateTime.class, ZonedDateTime.now(), (actual, expected) -> assertThat(actual.isEqual(expected)).isTrue(), "TIMESTAMP WITH TIME ZONE");
    }

    private static <T> Mono<T> close(Connection connection) {
        return Mono.from(connection
            .close())
            .then(Mono.empty());
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType) {
        testCodec(javaType, value, (actual, expected) -> assertThat(actual).isEqualTo(expected), sqlType);
    }

    private <T> void testCodec(Class<T> javaType, T value, BiConsumer<T, T> equality, String sqlType) {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test ( value %s )", sqlType));

        try {
            this.connectionFactory.create()
                .flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES ($1)")
                    .bind("$1", value)
                    .execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            this.connectionFactory.create()
                .flatMapMany(connection -> connection

                    .createStatement("SELECT value FROM test")
                    .execute()

                    .map(result -> result.map((row, metadata) -> row.get("value", javaType)))
                    .flatMap(Function.identity())

                    .concatWith(close(connection)))
                .as(StepVerifier::create)
                .assertNext(r2dbc -> equality.accept(r2dbc, value))
                .verifyComplete();
        } finally {
            //SERVER.getJdbcOperations().execute("DROP TABLE test");
        }
    }

}
