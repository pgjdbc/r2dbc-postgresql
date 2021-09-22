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
import io.r2dbc.postgresql.client.Parameter;
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

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR_ARRAY;
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
    void delegatePriority() {
        assertThat(codecs.decode(TEST.buffer(2).writeShort((byte) 100), INT2.getObjectId(), FORMAT_BINARY, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "100"), INT2.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "test"), VARCHAR.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(String.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"), TIMESTAMP.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(LocalDateTime.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-05 00:35:43.048593+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(OffsetDateTime.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT2_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Short[]{100, 200});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Integer[]{100, 200});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new Long[]{100L, 200L});
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "{alpha,bravo}"), VARCHAR_ARRAY.getObjectId(), FORMAT_TEXT, Object.class)).isEqualTo(new String[]{"alpha", "bravo"});
    }

    @Test
    void encode() {
        Parameter parameter = this.codecs.encode(100);

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
        Parameter parameter = this.codecs.encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(new Parameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
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
