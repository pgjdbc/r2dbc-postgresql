/*
 * Copyright 2019-2020 the original author or authors.
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
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BinaryByteArrayCodecTest {

    private static final int dataType = BYTEA.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BinaryByteArrayCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        BinaryByteArrayCodec codec = new BinaryByteArrayCodec(TEST);

        byte[] binaryArray = codec.decode(encode(TEST, "\\\\x746573742d76616c7565"), dataType, FORMAT_TEXT, byte[].class);
        assertThat(binaryArray).isEqualTo("test-value".getBytes());
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BinaryByteArrayCodec(TEST).decode(null, dataType, FORMAT_TEXT, byte[].class)).isNull();
    }

    @Test
    void doCanDecode() {
        BinaryByteArrayCodec codec = new BinaryByteArrayCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(BYTEA, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BinaryByteArrayCodec(TEST).doCanDecode(BYTEA, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BinaryByteArrayCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doCanEncode() {
        assertThat(new BinaryByteArrayCodec(TEST).canEncode(new byte[0])).isTrue();
    }

    @Test
    void doEncode() {
        ParameterAssert.assertThat(new BinaryByteArrayCodec(TEST).doEncode("test-value".getBytes()))
            .hasFormat(FORMAT_TEXT)
            .hasType(BYTEA.getObjectId())
            .hasValue(encode(TEST, "\\x746573742d76616c7565"));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BinaryByteArrayCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new BinaryByteArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, BYTEA.getObjectId(), NULL_VALUE));
    }

}
