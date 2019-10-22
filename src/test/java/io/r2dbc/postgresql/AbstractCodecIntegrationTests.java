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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.Buffer;
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
import java.util.function.Consumer;
import java.util.function.Function;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

abstract class AbstractCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void bigDecimal() {
        testCodec(BigDecimal.class, new BigDecimal("1000.00"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("-1"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("10000.0000023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("10010.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("2000010010.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("0"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT2");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT4");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT8");
        testCodec(BigDecimal.class, new BigDecimal("100"), "FLOAT4");
        testCodec(BigDecimal.class, new BigDecimal("100"), "FLOAT8");
    }

    @Test
    void booleanPrimitive() {
        testCodec(Boolean.class, true, "BOOL");
    }

    @Test
    void binary() {
        testCodec(ByteBuffer.class, ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), "BYTEA");
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
    void blob() {
        byte[] bytes = {1, 2, 3, 4};
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
                Flux.from(actual.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes),
                Flux.from(expected.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes)
            )
                .as(StepVerifier::create)
                .assertNext(t -> assertThat(t.getT1()).isEqualTo(t.getT2()))
                .verifyComplete()
            , "BYTEA");
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
    void date() {
        testCodec(Date.class, new Date(), "TIMESTAMP");
    }

    @Test
    void doublePrimitive() {
        testCodec(Double.class, 100.0, "INT2");
        testCodec(Double.class, 100.0, "INT4");
        testCodec(Double.class, 100.0, "INT8");
        testCodec(Double.class, 100.1, "DECIMAL");
        testCodec(Double.class, 100.1, "FLOAT4");
        testCodec(Double.class, 100.1, "FLOAT8");
    }

    @Test
    void floatPrimitive() {
        testCodec(Float.class, 100.0f, "INT2");
        testCodec(Float.class, 100.0f, "INT4");
        testCodec(Float.class, 100.0f, "INT8");
        testCodec(Float.class, 100.1f, "DECIMAL");
        testCodec(Float.class, 100.1f, "FLOAT4");
        testCodec(Float.class, 100.1f, "FLOAT8");
    }

    @Test
    void inetAddress() throws UnknownHostException {
        testCodec(InetAddress.class, InetAddress.getLocalHost(), "INET");
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
        testCodec(Integer.class, 100, "INT2");
        testCodec(Integer.class, 100, "INT4");
        testCodec(Integer.class, 100, "INT8");
        testCodec(Integer.class, 100, "NUMERIC");
        testCodec(Integer.class, 100, "FLOAT4");
        testCodec(Integer.class, 100, "FLOAT8");
    }

    @Test
    void intTwoDimensionalArray() {
        testCodec(Integer[][].class, new Integer[][]{{100, 200}, {300, null}}, "INT4[][]");
    }

    @Test
    void json() {
        testCodec(String.class, "{\"hello\": \"world\"}", "JSON", "$1::json");

        testCodec(Json.class, Json.of("{\"hello\": \"world\"}"), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSON");
        testCodec(Json.class, Json.of("{\"hello\": \"world\"}".getBytes()), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSON");
        testCodec(Json.class, Json.of(ByteBuffer.wrap("{\"hello\": \"world\"}".getBytes())), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSON");
        testCodec(Json.class, Json.of(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes())), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSON");
        testCodec(Json.class, Json.of(new ByteBufInputStream(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes()), true)), (actual, expected) -> assertThat(actual.asString()).isEqualTo((
            "{\"hello\": \"world\"}")), "JSON");

        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), String.class, "{\"hello\": \"world\"}", "JSON");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), byte[].class, "{\"hello\": \"world\"}".getBytes(), "JSON");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), ByteBuffer.class, ByteBuffer.wrap("{\"hello\": \"world\"}".getBytes()), "JSON");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), ByteBuf.class, (Consumer<ByteBuf>) actual -> {

            assertThat(actual).isEqualTo(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes()));
            actual.release();

        }, "JSON");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), InputStream.class, (Consumer<InputStream>) actual -> {
            try {
                assertThat(StreamUtils.copyToByteArray(actual)).isEqualTo("{\"hello\": \"world\"}".getBytes());
                actual.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "JSON");
    }

    @Test
    void jsonb() {
        testCodec(String.class, "{\"hello\": \"world\"}", "JSONB", "$1::json");

        testCodec(Json.class, Json.of("{\"hello\": \"world\"}"), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSONB");
        testCodec(Json.class, Json.of("{\"hello\": \"world\"}".getBytes()), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSONB");
        testCodec(Json.class, Json.of(ByteBuffer.wrap("{\"hello\": \"world\"}".getBytes())), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSONB");
        testCodec(Json.class, Json.of(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes())), (actual, expected) -> assertThat(actual.asString()).isEqualTo(("{\"hello\": \"world\"}")), "JSONB");
        testCodec(Json.class, Json.of(new ByteBufInputStream(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes()), true)), (actual, expected) -> assertThat(actual.asString()).isEqualTo((
            "{\"hello\": \"world\"}")), "JSONB");

        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), String.class, "{\"hello\": \"world\"}", "JSONB");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), byte[].class, "{\"hello\": \"world\"}".getBytes(), "JSONB");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), ByteBuffer.class, ByteBuffer.wrap("{\"hello\": \"world\"}".getBytes()), "JSONB");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), ByteBuf.class, (Consumer<ByteBuf>) actual -> {

            assertThat(actual).isEqualTo(Unpooled.wrappedBuffer("{\"hello\": \"world\"}".getBytes()));
            actual.release();

        }, "JSONB");
        testCodecReadAs(Json.of("{\"hello\": \"world\"}"), InputStream.class, (Consumer<InputStream>) actual -> {
            try {
                assertThat(StreamUtils.copyToByteArray(actual)).isEqualTo("{\"hello\": \"world\"}".getBytes());
                actual.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "JSONB");
    }

    @Test
    void localDate() {
        testCodec(LocalDate.class, LocalDate.now(), "DATE");
        testCodec(LocalDate.class, LocalDate.now(), "TIMESTAMP");
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
        testCodec(Long.class, 100L, "INT2");
        testCodec(Long.class, 100L, "INT4");
        testCodec(Long.class, 100L, "INT8");
        testCodec(Long.class, 100L, "NUMERIC");
        testCodec(Long.class, 100L, "FLOAT4");
        testCodec(Long.class, 100L, "FLOAT8");
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
        testCodec(String[].class, new String[]{"NULL", "", "test value3", "hello\\world"}, "VARCHAR[]");
        testCodec(String[].class, new String[]{"NULL", "", "test value3", "hello\\world"}, "TEXT[]");
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
        testCodec(ZonedDateTime.class, ZonedDateTime.now(), (actual, expected) -> {
            assertThat(actual.toLocalDateTime()).isEqualTo(expected.toLocalDateTime());
        }, "TIMESTAMP WITH TIME ZONE");
    }

    private static <T> Mono<T> close(Connection connection) {
        return Mono.from(connection
            .close())
            .then(Mono.empty());
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType) {
        testCodec(javaType, value, sqlType, "$1");
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType, String insertPlaceholder) {
        testCodec(javaType, value, (actual, expected) -> {

            if (value instanceof Float) {
                assertThat((Float) actual).isCloseTo((Float) value, Offset.offset(0.01f));
                return;
            }

            if (value instanceof Double) {
                assertThat((Double) actual).isCloseTo((Double) value, Offset.offset(0.01));
                return;
            }
            assertThat(actual).isEqualTo(expected);
        }, sqlType, insertPlaceholder);
    }

    private <T> void testCodec(Class<T> javaType, T value, BiConsumer<T, T> equality, String sqlType) {
        testCodec(javaType, value, equality, sqlType, "$1");
    }

    private <T> void testCodec(Class<T> javaType, T value, BiConsumer<T, T> equality, String sqlType, String insertPlaceholder) {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test ( value %s )", sqlType));

        try {
            this.connectionFactory.create()
                .flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")")
                    .bindNull("$1", javaType)
                    .execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            SERVER.getJdbcOperations().execute("DELETE FROM test");

            this.connectionFactory.create()
                .flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")")
                    .bind("$1", value)
                    .execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();


            if (value instanceof Buffer) {
                ((Buffer) value).rewind();
            }

            this.connectionFactory.create()
                .flatMapMany(connection -> {

                    PostgresqlStatement statement;
                    if (insertPlaceholder.equals("$1")) {
                        statement = connection
                            // where clause added to force using extended query instead of simple query
                            .createStatement("SELECT value FROM test WHERE " + insertPlaceholder + " <> 1")
                            .bind("$1", 2);
                    } else {
                        statement = connection.createStatement("SELECT value FROM test");
                    }
                    return statement.execute()

                        .map(result -> result.map((row, metadata) -> row.get("value", javaType)))
                        .flatMap(Function.identity())

                        .concatWith(close(connection));
                })
                .as(StepVerifier::create)
                .assertNext(r2dbc -> equality.accept(r2dbc, value))
                .verifyComplete();
        } finally {
            SERVER.getJdbcOperations().execute("DROP TABLE test");
        }
    }


    private <T> void testRead(Class<T> javaType, T value, String sqlType, String insertPlaceholder) {
        testCodec(javaType, value, (actual, expected) -> {

            if (value instanceof Float) {
                assertThat((Float) actual).isCloseTo((Float) value, Offset.offset(0.01f));
                return;
            }

            if (value instanceof Double) {
                assertThat((Double) actual).isCloseTo((Double) value, Offset.offset(0.01));
                return;
            }
            assertThat(actual).isEqualTo(expected);
        }, sqlType, insertPlaceholder);
    }

    private <W, R> void testCodecReadAs(W toWrite, Class<R> javaTypeToRead, R expected, String sqlType) {
        testCodecReadAs(toWrite, javaTypeToRead, (Consumer<R>) (actual) -> assertThat(actual).isEqualTo(expected), sqlType);
    }

    private <W, R> void testCodecReadAs(W toWrite, Class<R> javaTypeToRead, Consumer<R> equality, String sqlType) {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test ( value %s )", sqlType));

        try {
            this.connectionFactory.create()
                .flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES ($1)")
                    .bind("$1", toWrite)
                    .execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection)))
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

            this.connectionFactory.create()
                .flatMapMany(connection -> {
                    return connection.createStatement("SELECT value FROM test").execute()

                        .map(result -> result.map((row, metadata) -> row.get("value", javaTypeToRead)))
                        .flatMap(Function.identity())

                        .concatWith(close(connection));
                })
                .as(StepVerifier::create)
                .assertNext(equality)
                .verifyComplete();
        } finally {
            SERVER.getJdbcOperations().execute("DROP TABLE test");
        }
    }

}
