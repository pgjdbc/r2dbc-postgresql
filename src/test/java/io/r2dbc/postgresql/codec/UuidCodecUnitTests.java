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

import io.r2dbc.postgresql.client.EncodedParameter;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link UuidCodec}.
 */
final class UuidCodecUnitTests {

    private static final int dataType = PostgresqlObjectId.UUID.getObjectId();

    private static final UUID u1 = UUID.randomUUID();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        UUID uuid = UUID.randomUUID();

        assertThat(new UuidCodec(TEST).decode(encode(TEST, uuid.toString()), dataType, FORMAT_TEXT, UUID.class))
            .isEqualTo(uuid);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new UuidCodec(TEST).decode(null, dataType, FORMAT_TEXT, UUID.class)).isNull();
    }

    @Test
    void doCanDecode() {
        UuidCodec codec = new UuidCodec(TEST);

        assertThat(codec.doCanDecode(PostgresqlObjectId.UUID, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(PostgresqlObjectId.UUID, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        UUID uuid = UUID.randomUUID();

        assertThat(new UuidCodec(TEST).doEncode(uuid))
            .hasFormat(FORMAT_TEXT)
            .hasType(PostgresqlObjectId.UUID.getObjectId())
            .hasValue(encode(TEST, uuid.toString()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeItem() {
        assertThat(new UuidCodec(TEST).doEncodeText(u1)).isEqualTo(u1.toString());
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidCodec(TEST).doEncodeText(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new UuidCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, PostgresqlObjectId.UUID.getObjectId(), NULL_VALUE));
    }

}
