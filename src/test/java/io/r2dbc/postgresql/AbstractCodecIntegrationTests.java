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
import io.r2dbc.postgresql.codec.*;
import io.r2dbc.spi.*;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integrations tests for our built-in codecs.
 * Executed typically in text and binary mode through subclasses.
 *
 * @see CodecBinaryFormatIntegrationTests
 * @see CodecTextFormatIntegrationTests
 */
abstract class AbstractCodecIntegrationTests extends AbstractIntegrationTests {

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        try {
            SERVER.getJdbcOperations().execute("CREATE TYPE my_enum AS ENUM ('HELLO', 'WORLD')");
        } catch (DataAccessException e) {
            // ignore duplicate types
        }
        builder.codecRegistrar(EnumCodec.builder().withEnum("my_enum", MyEnum.class).build());
    }

    @Test
    void bigDecimal() {
        testCodec(BigDecimal.class, new BigDecimal("1000.00"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("-1"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("10000.0000023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("10010.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("-10010.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("2000010010.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("0.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("-0.1200023"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("0"), "NUMERIC");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT2");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT4");
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT8");
        testCodec(BigDecimal.class, new BigDecimal("100"), "FLOAT4");
        testCodec(BigDecimal.class, new BigDecimal("100"), "FLOAT8");
        testCodec(BigDecimal.class, new BigDecimal("100"), "NUMERIC", R2dbcType.NUMERIC);
        testCodec(BigDecimal.class, new BigDecimal("100"), "FLOAT8", R2dbcType.DOUBLE);
        testCodec(BigDecimal.class, new BigDecimal("100"), "INT8", R2dbcType.BIGINT);
    }

    @Test
    void bigInteger() {
        testCodec(BigInteger.class, new BigInteger("1000"), "NUMERIC");
        testCodec(BigInteger.class, new BigInteger("-1"), "NUMERIC");
        testCodecReadAs(new BigDecimal("10000.0000023"), BigInteger.class, new BigInteger("10000"), "NUMERIC");
        testCodecReadAs(new BigDecimal("10010.1200023"), BigInteger.class, new BigInteger("10010"), "NUMERIC");
        testCodecReadAs(new BigDecimal("2000010010.1200023"), BigInteger.class, new BigInteger("2000010010"), "NUMERIC");
        testCodec(BigInteger.class, new BigInteger("0"), "NUMERIC");
        testCodec(BigInteger.class, new BigInteger("100"), "INT2");
        testCodec(BigInteger.class, new BigInteger("100"), "INT4");
        testCodec(BigInteger.class, new BigInteger("100"), "INT8");
        testCodec(BigInteger.class, new BigInteger("100"), "FLOAT4");
        testCodec(BigInteger.class, new BigInteger("100"), "FLOAT8");
    }

    @Test
    void booleanArray() {
        testCodec(Boolean[].class, new Boolean[]{true, false, true}, "BOOL[]");
    }

    @Test
    void booleanPrimitive() {
        testCodec(Boolean.class, true, "BOOL");
    }

    @Test
    void booleanTwoDimensionalArray() {
        testCodec(Boolean[][].class, new Boolean[][]{{false, true}, {true, null}}, "BOOL[][]");
    }

    @Test
    void binary() {
        testCodec(ByteBuffer.class, ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), "BYTEA");
        testCodec(ByteBuffer.class, ByteBuffer.wrap(new byte[]{}), "BYTEA");
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
        BiConsumer<Blob, Blob> equality = (actual, expected) -> Flux.zip(Flux.from(actual.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes),
            Flux.from(expected.stream()).reduce(TEST.heapBuffer(), ByteBuf::writeBytes)).as(StepVerifier::create).assertNext(t -> {
            try {
                assertThat(t.getT1()).isEqualTo(t.getT2());
            } finally {
                t.getT1().release();
                t.getT2().release();
            }
        }).verifyComplete();

        Function<byte[], Blob> byteToBlob = (bytes) -> new Blob() {

            @Override
            public Publisher<Void> discard() {
                return Mono.empty();
            }

            @Override
            public Publisher<ByteBuffer> stream() {
                return Mono.just(ByteBuffer.wrap(bytes));
            }
        };

        testCodec(Blob.class, byteToBlob.apply(new byte[]{1, 2, 3, 4}), equality, "BYTEA");
        testCodec(Blob.class, byteToBlob.apply(new byte[]{}), equality, "BYTEA");
    }

    @Test
    void circleArray() {
        testCodec(Circle[].class, new Circle[]{Circle.of(Point.of(1.12, 2.12), 3.12), Circle.of(Point.of(Double.MIN_VALUE, Double.MIN_VALUE), Double.MAX_VALUE)}, "CIRCLE[]");
    }

    @Test
    void circle() {
        testCodec(Circle.class, Circle.of(Point.of(1.12, 2.12), 3.12), "CIRCLE");
        testCodec(Circle.class, Circle.of(Point.of(Double.MIN_VALUE, Double.MIN_VALUE), Double.MAX_VALUE), "CIRCLE");
    }

    @Test
    void circleTwoDimensionalArray() {
        testCodec(Circle[][].class, new Circle[][]{{Circle.of(Point.of(1.12, 2.12), 3.12), Circle.of(Point.of(Double.MIN_VALUE, Double.MIN_VALUE), Double.MAX_VALUE)}, {Circle.of(Point.of(-2.4,
            -456.2), 20), null}}, "CIRCLE[][]");
    }

    @Test
    void clob() {
        testCodec(Clob.class, new Clob() {

            @Override
            public Publisher<Void> discard() {
                return Mono.empty();
            }

            @Override
            public Publisher<CharSequence> stream() {
                return Mono.just("test-value");
            }
        }, (actual, expected) -> Flux.zip(Flux.from(actual.stream()).reduce(new StringBuilder(), StringBuilder::append).map(StringBuilder::toString),
            Flux.from(expected.stream()).reduce(new StringBuilder(), StringBuilder::append).map(StringBuilder::toString)).as(StepVerifier::create).assertNext(t -> assertThat(t.getT1()).isEqualToIgnoringWhitespace(t.getT2())).verifyComplete(), "TEXT");
    }

    @Test
    void date() {
        testCodec(Date.class, new Date(), "TIMESTAMP");
    }

    @Test
    void dateArray() {
        testCodec(Date[].class, new Date[]{new Date(0), new Date()}, "TIMESTAMP[]");
        testCodec(Date[].class, new Date[]{new Date(0), new Date()}, LocalDateTime[].class, (actual, expected) -> {
            assertThat(actual[0]).isAfter(LocalDateTime.of(1969, 1, 1, 0, 0)).isBefore(LocalDateTime.of(1971, 1, 1, 0, 0));
        }, "TIMESTAMP[]", "$1", null);
    }

    @Test
    void doubleArray() {
        testCodec(Double[].class, new Double[]{100.5, 200.25, 300.125}, "FLOAT8[]");
        testCodec(Double[].class, new Double[]{100.5, 200.25, 300.125}, Float[].class, (actual, expected) -> {
            assertThat(actual).isEqualTo(new Float[]{100.5f, 200.25f, 300.125f});
        }, "FLOAT4[]", "$1", null);
    }

    @Test
    void doublePrimitive() {
        testCodec(Double.class, 100.0, "INT2");
        testCodec(Double.class, 100.0, "INT4");
        testCodec(Double.class, 100.0, "INT8");
        testCodec(Double.class, 100.1, "DECIMAL");
        testCodec(Double.class, 100.1, "FLOAT4");
        testCodec(Double.class, 100.1, "FLOAT8");

        testCodec(Double.class, 100.1, "DECIMAL", R2dbcType.DECIMAL);
        testCodec(Double.class, 100.1, "FLOAT4", R2dbcType.FLOAT);
        testCodec(Double.class, 100.1, "FLOAT8", R2dbcType.DOUBLE);
    }

    @Test
    void doubleTwoDimensionalArray() {
        testCodec(Double[][].class, new Double[][]{{100.5, 200.25}, {300.125, null}}, "FLOAT8[][]");
    }

    @Test
    void simpleMappedEnum() {
        testCodec(MyEnum.class, MyEnum.HELLO, "my_enum");
    }

    @Test
    void mappedEnumArray() {
        testCodec(MyEnum[].class, MyEnum.values(), "my_enum[]");
    }

    @Test
    void floatArray() {
        testCodec(Float[].class, new Float[]{100.5f, 200.25f, 300.125f}, "FLOAT4[]");
    }

    @Test
    void floatPrimitive() {
        testCodec(Float.class, 100.0f, "INT2");
        testCodec(Float.class, 100.0f, "INT4");
        testCodec(Float.class, 100.0f, "INT8");
        testCodec(Float.class, 100.1f, "DECIMAL");
        testCodec(Float.class, 100.1f, "FLOAT4");
        testCodec(Float.class, 100.1f, "FLOAT8");

        testCodec(Float.class, 100.1f, "DECIMAL", R2dbcType.DECIMAL);
        testCodec(Float.class, 100.1f, "FLOAT4", R2dbcType.FLOAT);
        testCodec(Float.class, 100.1f, "FLOAT8", R2dbcType.DOUBLE);
    }

    @Test
    void floatTwoDimensionalArray() {
        testCodec(Float[][].class, new Float[][]{{100.5f, 200.25f}, {300.125f, null}}, "FLOAT4[][]");
    }

    @Test
    void hstore() {
        SERVER.getJdbcOperations().execute("CREATE EXTENSION IF NOT EXISTS hstore");
        Map<String, String> hstore = new LinkedHashMap<>();
        hstore.put("hello", "world");
        hstore.put("key\"with quote", "value\" with quote");
        hstore.put("null-value", null);
        testCodec(Map.class, hstore, "HSTORE");
    }

    @Test
    void inetAddress() throws UnknownHostException {
        testCodec(InetAddress.class, InetAddress.getLocalHost(), "INET");
    }

    @Test
    void inetAddressArray() throws UnknownHostException {
        testCodec(InetAddress[].class, new InetAddress[]{InetAddress.getLocalHost()}, "INET[]");
    }

    @Test
    void instant() {
        testCodec(Instant.class, Instant.now().truncatedTo(ChronoUnit.MICROS), "TIMESTAMPTZ");
    }

    @Test
    void intArray() {
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "INT2[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "INT4[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "INT8[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "OID[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "NUMERIC[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "FLOAT4[]");
        testCodec(Integer[].class, new Integer[]{100, 200, 300}, "FLOAT8[]");
    }

    @Test
    void intPrimitive() {
        testCodec(Integer.class, 100, "INT2");
        testCodec(Integer.class, 100, "INT4");
        testCodec(Integer.class, 100, "INT8");
        testCodec(Integer.class, 100, "OID");
        testCodec(Integer.class, 100, "NUMERIC");
        testCodec(Integer.class, 100, "FLOAT4");
        testCodec(Integer.class, 100, "FLOAT8");

        testCodec(Integer.class, 100, "INT2", R2dbcType.SMALLINT);
        testCodec(Integer.class, 100, "INT4", R2dbcType.INTEGER);
        testCodec(Integer.class, 100, "INT8", R2dbcType.INTEGER);
        testCodec(Long.class, 2314556683L, Integer.class, (actual, expected) -> assertThat(actual).isEqualTo(-1980410613), "OID", "$1", null);
        testCodec(Integer.class, 100, "NUMERIC", R2dbcType.NUMERIC);
        testCodec(Integer.class, 100, "FLOAT4", R2dbcType.FLOAT);
        testCodec(Integer.class, 100, "FLOAT8", R2dbcType.DOUBLE);
    }

    @Test
    void intTwoDimensionalArray() {
        testCodec(Integer[][].class, new Integer[][]{{100, 200}, {300, null}}, "INT4[][]");
    }

    @Test
    void json() {
        testCodec(String.class, "{\"hello\": \"world\"}", "JSON", "$1::json");

        testCodec(String.class, "{\"hello\": \"world\"}", "JSON", PostgresqlObjectId.JSON);

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
        testCodec(LocalDateTime.class, LocalDateTime.now().truncatedTo(ChronoUnit.MICROS), "TIMESTAMP");
        testCodec(LocalDateTime.class, LocalDateTime.now().truncatedTo(ChronoUnit.MICROS), "TIMESTAMPTZ");

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        LocalDateTime ldt = now.atZone(ZoneId.systemDefault()).toLocalDateTime();
        testCodec(LocalDateTime.class, ldt, Instant.class, (actual, expected) -> {

            assertThat(actual.atZone(ZoneId.systemDefault()).toLocalDateTime()).isEqualTo(expected);
        }, "TIMESTAMP", "$1", null);

        testCodec(Instant.class, now, LocalDateTime.class, (actual, expected) -> {

            assertThat(actual).isEqualTo(expected.atZone(ZoneId.systemDefault()).toLocalDateTime());
        }, "TIMESTAMP", "$1", null);

        testCodec(LocalDateTime.class, ldt, Instant.class, (actual, expected) -> {

            assertThat(actual.atZone(ZoneId.systemDefault()).toLocalDateTime()).isEqualTo(expected);
        }, "TIMESTAMPTZ", "$1", null);

        testCodec(Instant.class, now, LocalDateTime.class, (actual, expected) -> {

            assertThat(actual).isEqualTo(expected.atZone(ZoneId.systemDefault()).toLocalDateTime());
        }, "TIMESTAMPTZ", "$1", null);
    }

    @Test
    void localDateTimeArray() {
        testCodec(LocalDateTime[].class, new LocalDateTime[]{LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)}, "TIMESTAMP[]");
        testCodec(LocalDateTime[].class, new LocalDateTime[]{LocalDateTime.now().truncatedTo(ChronoUnit.MICROS)}, "TIMESTAMPTZ[]");
    }

    @Test
    void localTime() {
        testCodec(LocalTime.class, LocalTime.now().truncatedTo(ChronoUnit.MICROS), "TIME");
    }

    @Test
    void localTimeArray() {
        testCodec(LocalTime[].class, new LocalTime[]{LocalTime.now().truncatedTo(ChronoUnit.MICROS)}, "TIME[]");
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
        testCodec(Long.class, 2314556683L, "OID");
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
        testCodec(OffsetDateTime.class, OffsetDateTime.now().truncatedTo(ChronoUnit.MICROS), (actual, expected) -> assertThat(actual.isEqual(expected)).isTrue(), "TIMESTAMP WITH TIME ZONE");
    }

    @Test
    void pointArray() {
        testCodec(Point[].class, new Point[]{Point.of(1.12, 2.12), Point.of(Integer.MIN_VALUE, Integer.MAX_VALUE), Point.of(Double.MIN_VALUE, Double.MAX_VALUE)}, "POINT[]");
    }

    @Test
    void point() {
        testCodec(Point.class, Point.of(1.12, 2.12), "POINT");
        testCodec(Point.class, Point.of(Integer.MIN_VALUE, Integer.MAX_VALUE), "POINT");
        testCodec(Point.class, Point.of(Double.MIN_VALUE, Double.MAX_VALUE), "POINT");
    }

    @Test
    void pointTwoDimensionalArray() {
        testCodec(Point[][].class, new Point[][]{{Point.of(1.12, 2.12), Point.of(Integer.MIN_VALUE, Integer.MAX_VALUE)}, {Point.of(Double.MIN_VALUE, Double.MAX_VALUE), null}}, "POINT[][]");
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
        testCodec(String[].class, new String[]{""}, "BPCHAR[]");
        testCodec(String[].class, new String[]{}, "BPCHAR[]");
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
    void year() {
        testCodec(Year.class, Year.now(), "INT4");
    }

    @Test
    void month() {
        IntStream.rangeClosed(1, 12).forEach(month -> testCodec(Month.class, Month.of(month), "INT4"));
    }

    @Test
    void dayOfWeek() {
        IntStream.rangeClosed(1, 7).forEach(month -> testCodec(DayOfWeek.class, DayOfWeek.of(month), "INT4"));
    }

    @Test
    void monthDay() {
        testCodec(MonthDay.class, MonthDay.now(), "VARCHAR");
    }

    @Test
    void yearMonth() {
        testCodec(YearMonth.class, YearMonth.now(), "VARCHAR");
    }

    @Test
    void uuidArray() {
        testCodec(UUID[].class, new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()}, "UUID[]");
    }

    @Test
    void zoneId() {
        testCodec(ZoneId.class, ZoneId.systemDefault(), "BPCHAR(32)");
        testCodec(ZoneId.class, ZoneId.systemDefault(), "VARCHAR(32)");
    }

    @Test
    void zonedDateTime() {
        testCodec(ZonedDateTime.class, ZonedDateTime.now().truncatedTo(ChronoUnit.MICROS), (actual, expected) -> {
            assertThat(actual.toLocalDateTime()).isEqualTo(expected.toLocalDateTime());
        }, "TIMESTAMP WITH TIME ZONE");
    }

    @Test
    void offsetTime() {
        testCodec(OffsetTime.class, OffsetTime.of(LocalTime.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.UTC), "TIMETZ");
        testCodec(OffsetTime.class, OffsetTime.of(LocalTime.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.ofHoursMinutes(1, 30)), "TIMETZ");
        testCodec(OffsetTime.class, OffsetTime.of(LocalTime.now().truncatedTo(ChronoUnit.MICROS), ZoneOffset.ofHoursMinutes(-3, -30)), "TIMETZ");
    }

    @Test
    void boxArray() {
        testCodec(Box[].class, new Box[]{Box.of(Point.of(1.9, 2.8), Point.of(3.7, 4.6)), Box.of(Point.of(1.5, 3.3), Point.of(5., 7.))}, "BOX[]");
    }

    @Test
    void box() {
        testCodec(Box.class, Box.of(Point.of(1.9, 2.8), Point.of(3.7, 4.6)), "BOX");
        testCodec(Box.class, Box.of(Point.of(1.5, 3.3), Point.of(5., 7.)), "BOX");
    }

    @Test
    void boxTwoDimensionalArray() {
        testCodec(Box[][].class, new Box[][]{{null, Box.of(Point.of(1.9, 2.8), Point.of(3.7, 4.6))}, {Box.of(Point.of(1.5, 3.3), Point.of(5., 7.)), null}}, "BOX[][]");
    }

    @Test
    void lineArray() {
        testCodec(Line[].class, new Line[]{Line.of(1., 2., 4.), Line.of(-10.42, 3.14, 5.24)}, "LINE[]");
    }

    @Test
    void line() {
        testCodec(Line.class, Line.of(1., 2., 4.), "LINE");
        testCodec(Line.class, Line.of(-10.42, 3.14, 5.24), "LINE");
    }

    @Test
    void lineTwoDimensionalArray() {
        testCodec(Line[][].class, new Line[][]{{Line.of(1., 2., 4.), Line.of(-10.42, 3.14, 5.24)}, {null, Line.of(3, 4, 5)}}, "LINE[][]");
    }

    @Test
    void lsegArray() {
        testCodec(Lseg[].class, new Lseg[]{Lseg.of(1.11, 2.22, 3.33, 4.44), Lseg.of(6.6, 3.5, 6.6, -2.36)}, "LSEG[]");
    }

    @Test
    void lseg() {
        testCodec(Lseg.class, Lseg.of(Point.of(1.11, 2.22), Point.of(3.33, 4.44)), "LSEG");
    }

    @Test
    void lsegTwoDimensionalArray() {
        testCodec(Lseg[][].class, new Lseg[][]{{null, Lseg.of(1.11, 2.22, 3.33, 4.44)}, {Lseg.of(6.6, 3.5, 6.6, -2.36), null}}, "LSEG[][]");
    }

    @Test
    void pathArray() {
        testCodec(Path[].class, new Path[]{Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(1.1, 2.2)), Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3)),
            Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3), Point.of(1.1, 2.2)), Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3),
            Point.of(-3.5, 0.0))}, "PATH[]");
    }

    @Test
    void path() {
        testCodec(Path.class, Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(1.1, 2.2)), "PATH");
        testCodec(Path.class, Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3)), "PATH");
        testCodec(Path.class, Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3), Point.of(1.1, 2.2)), "PATH");
        testCodec(Path.class, Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3), Point.of(-3.5, 0.0)), "PATH");
    }

    @Test
    void pathTwoDimensionalArray() {
        testCodec(Path[][].class, new Path[][]{{Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(1.1, 2.2)), null, Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10),
            Point.of(0.42, 5.3))}, {Path.closed(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(0.42, 5.3), Point.of(1.1, 2.2)), Path.open(Point.of(1.1, 2.2), Point.of(10.10, 10.10),
            Point.of(0.42, 5.3), Point.of(-3.5, 0.0)), null}}, "PATH[][]");
    }

    @Test
    void polygonArray() {
        testCodec(Polygon[].class, new Polygon[]{Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(.42, 5.3)), Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(.42,
            5.3), Point.of(-3.5, 0.))}, "POLYGON[]");
    }

    @Test
    void polygon() {
        testCodec(Polygon.class, Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(.42, 5.3)), "POLYGON");
        testCodec(Polygon.class, Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(.42, 5.3), Point.of(-3.5, 0.)), "POLYGON");
    }

    @Test
    void polygonTwoDimensionalArray() {
        testCodec(Polygon[][].class, new Polygon[][]{{null, Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10), Point.of(.42, 5.3))}, {Polygon.of(Point.of(1.1, 2.2), Point.of(10.10, 10.10),
            Point.of(.42, 5.3), Point.of(-3.5, 0.)), null}}, "POLYGON[][]");
    }

    private static <T> Mono<T> close(Connection connection) {
        return Mono.from(connection.close()).then(Mono.empty());
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType) {
        testCodec(javaType, value, sqlType, "$1");
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType, Type parameterType) {
        testCodec(javaType, value, sqlType, "$1", parameterType);
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
        }, sqlType, insertPlaceholder, null);
    }

    private <T> void testCodec(Class<T> javaType, T value, String sqlType, String insertPlaceholder, Type parameterType) {
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
        }, sqlType, insertPlaceholder, parameterType);
    }

    private <T> void testCodec(Class<T> javaType, T value, BiConsumer<T, T> equality, String sqlType) {
        testCodec(javaType, value, equality, sqlType, "$1", null);
    }

    private <T> void testCodec(Class<T> javaType, T value, BiConsumer<T, T> equality, String sqlType, String insertPlaceholder, @Nullable Type parameterType) {
        testCodec(javaType, value, javaType, equality, sqlType, insertPlaceholder, parameterType);
    }

    private <IN, OUT> void testCodec(Class<IN> javaType, IN value, Class<OUT> outType, BiConsumer<OUT, IN> equality, String sqlType, String insertPlaceholder, @Nullable Type parameterType) {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test ( value %s )", sqlType));

        try {

            if (parameterType == null) {

                this.connectionFactory.create().flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")").bindNull("$1", javaType).execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection))).as(StepVerifier::create).expectNext(1L).verifyComplete();

                SERVER.getJdbcOperations().execute("DELETE FROM test");

                this.connectionFactory.create().flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")").bind("$1", value).execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection))).as(StepVerifier::create).expectNext(1L).verifyComplete();
            } else {

                this.connectionFactory.create().flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")").bind("$1", Parameters.in(parameterType)).execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection))).as(StepVerifier::create).expectNext(1L).verifyComplete();

                SERVER.getJdbcOperations().execute("DELETE FROM test");

                this.connectionFactory.create().flatMapMany(connection -> connection

                    .createStatement("INSERT INTO test VALUES (" + insertPlaceholder + ")").bind("$1", Parameters.in(parameterType, value)).execute()

                    .flatMap(PostgresqlResult::getRowsUpdated)

                    .concatWith(close(connection))).as(StepVerifier::create).expectNext(1L).verifyComplete();
            }

            if (value instanceof Buffer) {
                ((Buffer) value).rewind();
            }

            this.connectionFactory.create().flatMapMany(connection -> {

                PostgresqlStatement statement;
                if (insertPlaceholder.equals("$1")) {
                    statement = connection
                        // where clause added to force using extended query instead of simple query
                        .createStatement("SELECT value FROM test WHERE " + insertPlaceholder + " <> 1").bind("$1", 2);
                } else {
                    statement = connection.createStatement("SELECT value FROM test");
                }
                return statement.execute()

                    .map(result -> result.map((row, metadata) -> row.get("value", outType))).flatMap(Function.identity())

                    .concatWith(close(connection));
            }).as(StepVerifier::create).assertNext(r2dbc -> equality.accept(r2dbc, value)).verifyComplete();
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
        }, sqlType, insertPlaceholder, null);
    }

    private <W, R> void testCodecReadAs(W toWrite, Class<R> javaTypeToRead, R expected, String sqlType) {
        testCodecReadAs(toWrite, javaTypeToRead, (Consumer<R>) (actual) -> assertThat(actual).isEqualTo(expected), sqlType);
    }

    private <W, R> void testCodecReadAs(W toWrite, Class<R> javaTypeToRead, Consumer<R> equality, String sqlType) {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute(String.format("CREATE TABLE test (value %s)", sqlType));

        try {
            this.connectionFactory.create().flatMapMany(connection -> connection

                .createStatement("INSERT INTO test VALUES ($1)").bind("$1", toWrite).execute()

                .flatMap(PostgresqlResult::getRowsUpdated)

                .concatWith(close(connection))).as(StepVerifier::create).expectNext(1L).verifyComplete();

            this.connectionFactory.create().flatMapMany(connection -> {
                return connection.createStatement("SELECT value FROM test").execute()

                    .map(result -> result.map((row, metadata) -> row.get("value", javaTypeToRead))).flatMap(Function.identity())

                    .concatWith(close(connection));
            }).as(StepVerifier::create).assertNext(equality).verifyComplete();
        } finally {
            SERVER.getJdbcOperations().execute("DROP TABLE test");
        }
    }

    enum MyEnum {
        HELLO, WORLD,
    }

}
