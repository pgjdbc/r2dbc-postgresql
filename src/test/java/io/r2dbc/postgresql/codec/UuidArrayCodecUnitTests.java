/*
 * Copyright 2020 the original author or authors.
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
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UUID_ARRAY;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link UuidArrayCodec}.
 */
final class UuidArrayCodecUnitTests {

    private static final int dataType = UUID_ARRAY.getObjectId();

    private static final UUID u1 = UUID.randomUUID();

    private static final UUID u2 = UUID.randomUUID();

    private static final String parms = "{" + u1 + "," + u2 + "}";

    private final ByteBuf BINARY_ARRAY = TEST
        .buffer()
        .writeInt(1)
        .writeInt(0)
        .writeInt(2951)
        .writeInt(2)
        .writeInt(2)
        .writeInt(16)
        .writeLong(u1.getMostSignificantBits())
        .writeLong(u1.getLeastSignificantBits())
        .writeInt(16)
        .writeLong(u2.getMostSignificantBits())
        .writeLong(u2.getLeastSignificantBits());

    @Test
    void decodeItem() {
        assertThat(new UuidArrayCodec(TEST).decode(BINARY_ARRAY, dataType, FORMAT_BINARY, UUID[].class)).isEqualTo(new UUID[]{u1, u2});
        assertThat(new UuidArrayCodec(TEST).decode(encode(TEST, parms), dataType, FORMAT_TEXT, UUID[].class)).isEqualTo(new UUID[]{u1, u2});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void decodeObject() {
        assertThat(((Codec) new UuidArrayCodec(TEST)).decode(BINARY_ARRAY, dataType, FORMAT_BINARY, Object.class)).isEqualTo(new UUID[]{u1, u2});
        assertThat(((Codec) new UuidArrayCodec(TEST)).decode(encode(TEST, parms), dataType, FORMAT_TEXT, Object.class)).isEqualTo(new UUID[]{u1, u2});
    }

    @Test
    void doCanDecode() {
        assertThat(new UuidArrayCodec(TEST).doCanDecode(PostgresqlObjectId.UUID, FORMAT_TEXT)).isFalse();
        assertThat(new UuidArrayCodec(TEST).doCanDecode(UUID_ARRAY, FORMAT_TEXT)).isTrue();
        assertThat(new UuidArrayCodec(TEST).doCanDecode(UUID_ARRAY, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidArrayCodec(TEST).doCanDecode(null, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new UuidArrayCodec(TEST).encodeArray(() -> encode(TEST, parms)))
            .hasFormat(FORMAT_TEXT)
            .hasType(UUID_ARRAY.getObjectId())
            .hasValue(encode(TEST, parms));
    }

    @Test
    void encodeItem() {
        assertThat(new UuidArrayCodec(TEST).doEncodeText(u1)).isEqualTo(u1.toString());
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new UuidArrayCodec(TEST).doEncodeText(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new UuidArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, UUID_ARRAY.getObjectId(), NULL_VALUE));
    }

}
