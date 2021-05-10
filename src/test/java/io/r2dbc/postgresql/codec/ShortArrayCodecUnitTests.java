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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ArrayCodec <Short>}.
 */
final class ShortArrayCodecUnitTests {

    private static final int dataType = INT2_ARRAY.getObjectId();

    private final ByteBuf BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1)
        .writeInt(0)
        .writeInt(21)
        .writeInt(2)
        .writeInt(2)
        .writeInt(2)
        .writeShort(100)
        .writeInt(2)
        .writeShort(200);

    ArrayCodec<Short> codec;

    @BeforeEach
    void setup() {
        codec = new ArrayCodec<>(TEST, INT2_ARRAY, new ShortCodec(TEST), Short.class);
    }

    @Test
    void decodeItem() {
        assertThat(codec.decode(BINARY_ARRAY, dataType, FORMAT_BINARY, Short[].class)).isEqualTo(new Short[]{100, 200});
        assertThat(codec.decode(encode(TEST, "{100,200}"), dataType, FORMAT_TEXT, Short[].class)).isEqualTo(new Short[]{100, 200});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        assertThat(((Codec) codec).decode(BINARY_ARRAY, dataType, FORMAT_BINARY, Object.class)).isEqualTo(new Short[]{100, 200});
        assertThat(((Codec) codec).decode(encode(TEST, "{100,200}"), dataType, FORMAT_TEXT, Object.class)).isEqualTo(new Short[]{100, 200});
    }

    @Test
    void doCanDecode() {
        assertThat(codec.doCanDecode(INT2, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(INT2_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(INT2_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void canEncode() {
        assertThat(codec.canEncode(new Short[0])).isTrue();
        assertThat(codec.canEncode(new UUID[0])).isFalse();
    }

    @Test
    void canEncodeNull() {
        assertThat(codec.canEncodeNull(Short[].class)).isTrue();
        assertThat(codec.canEncodeNull(UUID[].class)).isFalse();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(codec.encodeArray(() -> encode(TEST, "{100,200}"), INT2_ARRAY))
            .hasFormat(FORMAT_TEXT)
            .hasType(INT2_ARRAY.getObjectId())
            .hasValue(encode(TEST, "{100,200}"));
    }

    @Test
    void encodeNull() {
        assertThat(codec.encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT2_ARRAY.getObjectId(), NULL_VALUE));
    }

}
