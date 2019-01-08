/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class IntegerCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        IntegerCodec codec = new IntegerCodec(TEST);

        assertThat(codec.decode(TEST.buffer(4).writeInt(100), FORMAT_BINARY, Integer.class)).isEqualTo(100);
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "100"), FORMAT_TEXT, Integer.class)).isEqualTo(100);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new IntegerCodec(TEST).decode(null, FORMAT_BINARY, Integer.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerCodec(TEST).decode(TEST.buffer(0), null, Integer.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        IntegerCodec codec = new IntegerCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_BINARY, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, INT4)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_TEXT, INT4)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new IntegerCodec(TEST).doEncode(100))
            .isEqualTo(new Parameter(FORMAT_BINARY, INT4.getObjectId(), TEST.buffer(4).writeInt(100)));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IntegerCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new IntegerCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, INT4.getObjectId(), null));
    }

}
