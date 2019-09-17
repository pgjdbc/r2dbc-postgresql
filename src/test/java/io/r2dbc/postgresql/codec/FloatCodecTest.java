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

import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class FloatCodecTest {

    private static final int dataType = FLOAT4.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new FloatCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        FloatCodec codec = new FloatCodec(TEST);

        assertThat(codec.decode(TEST.buffer(4).writeFloat(100.0f), dataType, FORMAT_BINARY, Float.class)).isEqualTo(100.0f);
        assertThat(codec.decode(encode(TEST, "100.0"), dataType, FORMAT_TEXT, Float.class)).isEqualTo(100.0f);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new FloatCodec(TEST).decode(null, dataType, FORMAT_BINARY, Float.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new FloatCodec(TEST).decode(TEST.buffer(0), dataType, null, Float.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        FloatCodec codec = new FloatCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(FLOAT4, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(FLOAT4, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new FloatCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new FloatCodec(TEST).doEncode(100f))
            .hasFormat(FORMAT_BINARY)
            .hasType(FLOAT4.getObjectId())
            .hasValue(TEST.buffer(4).writeFloat(100));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new FloatCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new FloatCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, FLOAT4.getObjectId(), NULL_VALUE));
    }

}
