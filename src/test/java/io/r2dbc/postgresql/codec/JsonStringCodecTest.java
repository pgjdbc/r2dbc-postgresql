/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class JsonStringCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonStringCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        String json = "{\"name\": \"John Doe\"}";
        JsonStringCodec jsonCodec = new JsonStringCodec(TEST);
        String decodedBytes = jsonCodec.decode(ByteBufUtils.encode(TEST, json), JSON.getObjectId(), FORMAT_TEXT, String.class);

        assertThat(decodedBytes).isEqualTo(json);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new JsonStringCodec(TEST).decode(null, JSON.getObjectId(), FORMAT_TEXT, String.class)).isNull();
    }

    @Test
    void doCanDecode() {
        JsonStringCodec jsonCodec = new JsonStringCodec(TEST);

        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_TEXT)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSON, FORMAT_BINARY)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSONB, FORMAT_TEXT)).isTrue();
        assertThat(jsonCodec.doCanDecode(JSONB, FORMAT_BINARY)).isTrue();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(jsonCodec.doCanDecode(MONEY, FORMAT_BINARY)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonStringCodec(TEST).doCanDecode(JSON, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonStringCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        String json = "{\"name\":\"John Doe\"}";
        JsonStringCodec jsonCodec = new JsonStringCodec(TEST);

        assertThat(jsonCodec.doEncode(json))
            .hasFormat(FORMAT_TEXT)
            .hasType(JSON.getObjectId())
            .hasValue(ByteBufUtils.encode(TEST, json));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new JsonStringCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new JsonStringCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, JSONB.getObjectId(), NULL_VALUE));
    }

}
