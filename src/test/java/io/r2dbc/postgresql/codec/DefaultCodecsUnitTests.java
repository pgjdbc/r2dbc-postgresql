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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8_ARRAY;
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

/**
 * Unit tests for {@link DefaultCodecs}.
 */
final class DefaultCodecsUnitTests {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), null, Object.class))
            .withMessage("format must not be null");
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void with OID 23");
    }

    @Test
    void delegatePriority() {
        Codecs codecs = new DefaultCodecs(TEST);

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
    }

    @Test
    void encode() {
        EncodedParameter parameter = new DefaultCodecs(TEST).encode(100);

        assertThat(parameter)
            .hasFormat(FORMAT_BINARY)
            .hasType(INT4.getObjectId())
            .hasValue(TEST.buffer(4).writeInt(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        EncodedParameter parameter = new DefaultCodecs(TEST).encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void addCodecFirst() {
        DefaultCodecs codecs = new DefaultCodecs(TEST);
        codecs.addFirst(DummyCodec.INSTANCE);
        assertThat(codecs).startsWith(DummyCodec.INSTANCE);
    }

    @Test
    void addCodecLast() {
        DefaultCodecs codecs = new DefaultCodecs(TEST);
        codecs.addLast(DummyCodec.INSTANCE);
        assertThat(codecs).endsWith(DummyCodec.INSTANCE);
    }

    @Test
    void decodeDoubleAsFloatArray() {

        DefaultCodecs codecs = new DefaultCodecs(TEST);

        Float[] expected = {100.5f, 200.25f};
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), PostgresqlObjectId.FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Float[].class))
            .isEqualTo(expected);
    }

    @Test
    void decodeFloatAsDoubleArray() {

        DefaultCodecs codecs = new DefaultCodecs(TEST);

        Double[] expected = {100.5, 200.25};
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.25}"), FLOAT4_ARRAY.getObjectId(), FORMAT_TEXT, Double[].class))
            .isEqualTo(expected);
    }

    enum DummyCodec implements Codec<Object> {

        INSTANCE;

        @Override
        public boolean canDecode(int dataType, Format format, Class<?> type) {
            return false;
        }

        @Override
        public boolean canEncode(Object value) {
            return false;
        }

        @Override
        public boolean canEncodeNull(Class<?> type) {
            return false;
        }

        @Override
        public EncodedParameter encode(Object value) {
            return null;
        }

        @Override
        public EncodedParameter encode(Object value, int dataType) {
            return null;
        }

        @Override
        public EncodedParameter encodeNull() {
            return null;
        }

        private EncodedParameter encodeNull(int dataType) {
            return null;
        }

        @Override
        public Class<?> type() {
            return null;
        }

        @Override
        public Object decode(ByteBuf buffer, int dataType, Format format, Class<?> type) {
            return null;
        }
    }

}
