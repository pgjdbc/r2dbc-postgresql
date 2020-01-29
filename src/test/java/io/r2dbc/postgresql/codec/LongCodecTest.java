/*
 * Copyright 2017-2020 the original author or authors.
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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class LongCodecTest {

    private static final int dataType = INT8.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        LongCodec codec = new LongCodec(TEST);

        assertThat(codec.decode(TEST.buffer(8).writeLong(100L), dataType, FORMAT_BINARY, Long.class)).isEqualTo(100L);
        assertThat(codec.decode(encode(TEST, "100"), dataType, FORMAT_TEXT, Long.class)).isEqualTo(100L);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LongCodec(TEST).decode(null, dataType, FORMAT_BINARY, Long.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongCodec(TEST).decode(TEST.buffer(0), dataType, null, Long.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        LongCodec codec = new LongCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(INT8, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT8, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new LongCodec(TEST).doEncode(100L))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT8.getObjectId())
            .hasValue(TEST.buffer(8).writeLong(100));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LongCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new LongCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, INT8.getObjectId(), NULL_VALUE));
    }

}
