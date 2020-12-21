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

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ShortCodec}.
 */
final class ShortCodecUnitTests {

    private static final int dataType = INT2.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        ShortCodec codec = new ShortCodec(TEST);

        assertThat(codec.decode(TEST.buffer(2).writeShort((short) 100), dataType, FORMAT_BINARY, Short.class)).isEqualTo((short) 100);
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "100"), dataType, FORMAT_TEXT, Short.class)).isEqualTo((short) 100);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new ShortCodec(TEST).decode(null, dataType, FORMAT_BINARY, Short.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortCodec(TEST).decode(TEST.buffer(0), dataType, null, Short.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        ShortCodec codec = new ShortCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(INT2, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT2, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new ShortCodec(TEST).doEncode((short) 100))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT2.getObjectId())
            .hasValue(TEST.buffer(2).writeShort(100));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ShortCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT2.getObjectId(), NULL_VALUE));
    }

}
