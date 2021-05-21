/*
 * Copyright 2017 the original author or authors.
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

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POINT_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultCodecs}.
 */
@ExtendWith(MockitoExtension.class)
final class DefaultCodecsUnitTests {

    DefaultCodecs codecs = new DefaultCodecs(TEST);

    @Captor
    ArgumentCaptor<Predicate<Codec<?>>> predicateArgumentCaptor;

    @Captor
    ArgumentCaptor<Map<Integer, Codec<?>>> cacheArgumentCaptor;

    @Mock
    Codec<String> dummyCodec;

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        assertThat(codecs.decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(codecs.decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> codecs.decode(TEST.buffer(4), INT4.getObjectId(), null, Object.class))
            .withMessage("format must not be null");
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codecs.decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(codecs.decode(null, INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codecs.decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void with OID 23");
    }

    @Test
    void delegatePriority() {
        assertThat(codecs.decode(TEST.buffer(2).writeShort((byte) 100), INT2.getObjectId(), FORMAT_BINARY, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "100"), INT2.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "test"), VARCHAR.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(String.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"), TIMESTAMP.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(LocalDateTime.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-05 00:35:43.048593+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(OffsetDateTime.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{t,f}"), BOOL_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Boolean[]{true, false});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Float[]{100.5f, 200.25f});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Double[]{100.5, 200.25});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT2_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Short[]{100, 200});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Integer[]{100, 200});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{alpha,bravo}"), VARCHAR_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new String[]{"alpha", "bravo"});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{\"((1.2, 123.1), 10)\",NULL}"), CIRCLE_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Circle[]{Circle.of(Point.of(1.2, 123.1), 10), null});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{\"((-10.42,3.14),(10.42,-3.14))\",NULL}"), POLYGON_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Polygon[]{Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)), null});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{\"(1.12,2.12)\",\"(-2147483648,2147483647)\",NULL}"), POINT_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Point[]{Point.of(1.12, 2.12), Point.of(Integer.MIN_VALUE, Integer.MAX_VALUE), null});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{\"{ 5.5, 3.2, 8 }\",\"{3,4,5}\",NULL}"), LINE_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Line[]{Line.of(5.5, 3.2, 8), Line.of(3, 4, 5), null});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, " {(3.7,4.6),(1.9,2.8);(5,7),(1.5,3.3);NULL}"), BOX_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Box[]{Box.of(3.7, 4.6, 1.9, 2.8), Box.of(5, 7, 1.5, 3.3), null});
    }

    @Test
    void encode() {
        EncodedParameter parameter = codecs.encode(100);

        assertThat(parameter)
            .hasFormat(FORMAT_BINARY)
            .hasType(INT4.getObjectId())
            .hasValue(TEST.buffer(4).writeInt(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codecs.encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        EncodedParameter parameter = codecs.encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codecs.encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void addCodecFirst() {
        DefaultCodecs spyCodecs = spy(codecs);
        Codec<?> stringCodec = spyCodecs.findEncodeCodec("string");
        when(dummyCodec.canEncode("string")).thenReturn(true);
        spyCodecs.addFirst(dummyCodec);
        assertThat(spyCodecs).startsWith(dummyCodec);
        // Make sure the cache is invalidated and the overridden codec is now returned in place of the default one
        verify(spyCodecs).invalidateCaches();
        Codec<?> overriddenStringCodec = spyCodecs.findEncodeCodec("string");
        assertThat(overriddenStringCodec).isNotEqualTo(stringCodec).isEqualTo(dummyCodec);
    }

    @Test
    void addCodecLast() {
        DefaultCodecs spyCodecs = spy(codecs);
        spyCodecs.addLast(dummyCodec);
        assertThat(spyCodecs).endsWith(dummyCodec);
        verify(spyCodecs).invalidateCaches();
    }

    @Test
    void decodeDoubleAsFloatArray() {
        Float[] expected = {100.5f, 200.25f};
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), PostgresqlObjectId.FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Float[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeFloatAsDoubleArray() {
        Double[] expected = {100.5, 200.25};
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Double[].class))
            .isEqualTo(expected);
    }

    @Test
    void findCodec() {
        Map<Integer, Codec<?>> cache = new HashMap<>();
        // To count the number of time the predicate is called
        AtomicInteger predicateCount = new AtomicInteger(0);
        // First call the codec is not in the cache
        Codec<?> found = codecs.findCodec(123, cache, codec -> {
            predicateCount.incrementAndGet();
            return codec.canEncode("a string");
        });
        assertThat(found).isInstanceOf(StringCodec.class);
        assertThat(predicateCount.getAndSet(0)).isEqualTo(1);
        // Second call the codec is in the cache
        assertThat(codecs.findCodec(123, cache, codec -> codec.canEncode("another string"))).isEqualTo(found);
        assertThat(predicateCount.get()).isZero();
        assertThat(cache).containsEntry(123, found);
    }

    @Test
    void findDecodeCodecShort() {
        DefaultCodecs spyCodecs = spy(codecs);
        Codec<Short> shortCodec = spyCodecs.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, Short.class);
        assertThat(shortCodec).isNotNull();
        verify(spyCodecs).findCodec(anyInt(), cacheArgumentCaptor.capture(), predicateArgumentCaptor.capture());
        assertThat(cacheArgumentCaptor.getValue()).containsValue(shortCodec);
    }

    @Test
    void findDecodeCodecNotFound() {
        assertThat(codecs.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, DefaultCodecsUnitTests.class)).isNull();
    }

    @Test
    void findEncodeCodecDouble() {
        DefaultCodecs spyCodecs = spy(codecs);
        Codec<?> doubleCodec = spyCodecs.findEncodeCodec(1.2);
        assertThat(doubleCodec).isInstanceOf(DoubleCodec.class);
        verify(spyCodecs).findCodec(anyInt(), cacheArgumentCaptor.capture(), predicateArgumentCaptor.capture());
        assertThat(cacheArgumentCaptor.getValue()).containsValue(doubleCodec);
    }

    @Test
    void findEncodeCodecNotFound() {
        assertThat(codecs.findEncodeCodec(this)).isNull();
    }

    @Test
    void findEncodeNullCodecInteger() {
        DefaultCodecs spyCodecs = spy(codecs);
        Codec<?> intCodec = spyCodecs.findEncodeNullCodec(Integer.class);
        assertThat(intCodec).isInstanceOf(IntegerCodec.class);
        verify(spyCodecs).findCodec(anyInt(), cacheArgumentCaptor.capture(), predicateArgumentCaptor.capture());
        assertThat(cacheArgumentCaptor.getValue()).containsValue(intCodec);
    }

    @Test
    void findEncodeNullCodecNotFound() {
        assertThat(codecs.findEncodeNullCodec(DefaultCodecsUnitTests.class)).isNull();
    }

}
