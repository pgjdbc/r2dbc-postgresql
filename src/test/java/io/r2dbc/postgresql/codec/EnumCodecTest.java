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
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class EnumCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @SuppressWarnings("unchecked")
    @Test
    void decode() {
        TimeUnit timeUnit = TimeUnit.DAYS;

        assertThat(new EnumCodec(TEST).decode(encode(TEST, timeUnit.name()), FORMAT_TEXT, TimeUnit.class))
            .isEqualTo(timeUnit);
    }

    @SuppressWarnings("unchecked")
    @Test
    void decodeNoByteBuf() {
        assertThat(new EnumCodec(TEST).decode(null, FORMAT_TEXT, TimeUnit.class)).isNull();
    }

    @Test
    void doCanDecode() {
        EnumCodec codec = new EnumCodec(TEST);

        assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, TimeUnit.class)).isFalse();
        assertThat(codec.canDecode(MONEY.getObjectId(), FORMAT_TEXT, TimeUnit.class)).isFalse();
        assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_TEXT, TimeUnit.class)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec(TEST).decode(null, null, TimeUnit.class))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec(TEST).decode(null, FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        TimeUnit timeUnit = TimeUnit.DAYS;

        assertThat(new EnumCodec(TEST).encode(timeUnit))
            .isEqualTo(new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), encode(TEST, timeUnit.name())));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new EnumCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), null));
    }

}
