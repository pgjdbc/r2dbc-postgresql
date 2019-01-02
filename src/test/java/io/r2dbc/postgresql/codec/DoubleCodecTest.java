/*
 * Copyright 2017-2018 the original author or authors.
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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class DoubleCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DoubleCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        DoubleCodec codec = new DoubleCodec(TEST);

        assertThat(codec.decode(TEST.buffer(8).writeDouble(100.0d), FORMAT_BINARY, Double.class)).isEqualTo(100.0d);
        assertThat(codec.decode(ByteBufUtils.encode(TEST, "100.0"), FORMAT_TEXT, Double.class)).isEqualTo(100.0d);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new DoubleCodec(TEST).decode(null, FORMAT_BINARY, Double.class)).isNull();
    }

    @Test
    void decodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DoubleCodec(TEST).decode(TEST.buffer(0), null, Double.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecode() {
        DoubleCodec codec = new DoubleCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_BINARY, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, FLOAT8)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_TEXT, FLOAT8)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DoubleCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        assertThat(new DoubleCodec(TEST).doEncode(100d))
            .isEqualTo(new Parameter(FORMAT_BINARY, FLOAT8.getObjectId(), TEST.buffer(8).writeDouble(100)));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DoubleCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new DoubleCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, FLOAT8.getObjectId(), null));
    }

}
