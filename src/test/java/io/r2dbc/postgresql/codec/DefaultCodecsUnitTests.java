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

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.EnumSet;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POINT_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.lenient;

/**
 * Unit tests for {@link DefaultCodecs}.
 */
@ExtendWith(MockitoExtension.class)
final class DefaultCodecsUnitTests {

    DefaultCodecs codecs;

    @Mock
    AbstractCodec<String> dummyCodec;

    @BeforeEach
    void before() {
        this.codecs = new DefaultCodecs(TEST, false);
        lenient().doReturn(String.class).when(this.dummyCodec).type();
        lenient().doReturn(EnumSet.of(FORMAT_TEXT, FORMAT_BINARY)).when(this.dummyCodec).getFormats();
        lenient().doReturn(Collections.singleton(TEXT)).when(this.dummyCodec).getDataTypes();
    }

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(null, true))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        assertThat(this.codecs.decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(this.codecs.decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codecs.decode(TEST.buffer(4), INT4.getObjectId(), null, Object.class))
            .withMessage("format must not be null");
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codecs.decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(this.codecs.decode(null, INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codecs.decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void with OID 23");
    }

    @Test
    void decodeFallbackToVarcharCodec() {
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"), TIMESTAMP.getObjectId(), FORMAT_TEXT, String.class))
            .isEqualTo("2018-11-04 15:35:00.847108");
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "2018-11-05 00:35:43.048593+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, String.class))
            .isEqualTo("2018-11-05 00:35:43.048593+09");
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "ENUM_VALUE"), 123456, FORMAT_TEXT, String.class))
            .isEqualTo("ENUM_VALUE");
    }

    @Test
    void delegatePriority() {
        assertThat(this.codecs.decode(TEST.buffer(2).writeShort((byte) 100), INT2.getObjectId(), FORMAT_BINARY, Object.class)).isInstanceOf(Short.class);
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "100"), INT2.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(Short.class);
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "test"), VARCHAR.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(String.class);
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"), TIMESTAMP.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(LocalDateTime.class);
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "2018-11-05 00:35:43.048593+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(OffsetDateTime.class);
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{t,f}"), BOOL_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Boolean[]{true, false});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Float[]{100.5f, 200.25f});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Float[].class)).isEqualTo(new Float[]{100.5f, 200.25f});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Double[]{100.5, 200.25});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Double[].class)).isEqualTo(new Double[]{100.5, 200.25});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT2_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Short[]{100, 200});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Integer[]{100, 200});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{alpha,bravo}"), VARCHAR_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new String[]{"alpha", "bravo"});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{\"((1.2, 123.1), 10)\",NULL}"), CIRCLE_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Circle[]{Circle.of(Point.of(1.2, 123.1), 10), null});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{\"((-10.42,3.14),(10.42,-3.14))\",NULL}"), POLYGON_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Polygon[]{Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)), null});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{\"(1.12,2.12)\",\"(-2147483648,2147483647)\",NULL}"), POINT_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Point[]{Point.of(1.12, 2.12), Point.of(Integer.MIN_VALUE, Integer.MAX_VALUE), null});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{\"{ 5.5, 3.2, 8 }\",\"{3,4,5}\",NULL}"), LINE_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Line[]{Line.of(5.5, 3.2, 8), Line.of(3, 4, 5), null});
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, " {(3.7,4.6),(1.9,2.8);(5,7),(1.5,3.3);NULL}"), BOX_ARRAY.getObjectId(), FORMAT_TEXT, Object.class))
            .isEqualTo(new Box[]{Box.of(3.7, 4.6, 1.9, 2.8), Box.of(5, 7, 1.5, 3.3), null});
    }

    @Test
    void encode() {
        EncodedParameter parameter = this.codecs.encode(100);

        assertThat(parameter)
            .hasFormat(FORMAT_BINARY)
            .hasType(INT4.getObjectId())
            .hasValue(TEST.buffer(4).writeInt(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codecs.encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        EncodedParameter parameter = this.codecs.encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codecs.encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void addCodecFirst() {
        DefaultCodecLookup codecLookup = new DefaultCodecLookup(this.codecs);

        Codec<?> stringCodec = codecLookup.findEncodeCodec("string");
        lenient().when(this.dummyCodec.canEncode("string")).thenReturn(true);
        this.codecs.addFirst(this.dummyCodec);
        assertThat(this.codecs).startsWith(this.dummyCodec);
        Codec<?> overriddenStringCodec = codecLookup.findEncodeCodec("string");
        assertThat(overriddenStringCodec).isNotEqualTo(stringCodec).isEqualTo(this.dummyCodec);
    }

    @Test
    void addCodecLast() {
        this.codecs.addLast(this.dummyCodec);
        assertThat(this.codecs).endsWith(this.dummyCodec);
    }

    @Test
    void decodeDoubleAsFloatArray() {
        Float[] expected = {100.5f, 200.25f};
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Float[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeFloatAsDoubleArray() {
        Double[] expected = {100.5, 200.25};
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Double[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeArrayOfArray() {
        Double[][] expected = new Double[][]{{123.4, 567.8}, {12.3, 45.6}};
        assertThat(this.codecs.decode(ByteBufUtils.encode(TEST, "{{123.4, 567.8}, {12.3, 45.6}}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Double[][].class)).isEqualTo(expected);
    }

    @Test
    void testEncodeDecode() {
        Flux.fromIterable((new Binding(1)).add(0, this.codecs.encode(65.589)).getParameterValues())
            .flatMap(Flux::from)
            .subscribe(bb -> {
                assertThat(this.codecs.decode(bb, FLOAT8.getObjectId(), FORMAT_BINARY, Double.class)).isEqualTo(65.589);
            });
        StepVerifier.create(Flux.fromIterable((new Binding(2))
                    .add(0, this.codecs.encode(65.589))
                    .add(1, this.codecs.encode((short) 15))
                    .getParameterValues())
                .flatMap(Flux::from))
            .assertNext(byteBuf -> {
                assertThat(this.codecs.decode(byteBuf, FLOAT8.getObjectId(), FORMAT_BINARY, Double.class)).isEqualTo(65.589);
            })
            .assertNext(byteBuf -> {
                assertThat(this.codecs.decode(byteBuf, INT2.getObjectId(), FORMAT_BINARY, Short.class)).isEqualTo((short) 15);
            })
            .verifyComplete();
    }

}
