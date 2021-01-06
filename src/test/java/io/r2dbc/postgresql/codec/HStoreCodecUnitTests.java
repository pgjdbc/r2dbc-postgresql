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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import io.r2dbc.postgresql.message.Format;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link HStoreCodec}.
 */
final class HStoreCodecUnitTests {

    private static final int dataType = 20832;

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new HStoreCodec(null, dataType))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decodeAsText() {
        ByteBuf buffer = TEST.buffer();
        buffer.writeCharSequence("\"b\"=>\"\\\"2.2\", \"a\\\"\"=>\"1\", \"c\"=>NULL", Charset.defaultCharset());
        Map<String, String> res = new HStoreCodec(TEST, dataType).decode(buffer, dataType, Format.FORMAT_TEXT, Map.class);
        Map<String, String> expect = new HashMap<>();
        expect.put("a\"", "1");
        expect.put("b", "\"2.2");
        expect.put("c", null);

        Assertions.assertThat(res).isEqualTo(expect);
    }

    @Test
    void decode() {
        assertThat(new HStoreCodec(TEST, dataType).decode(TEST.buffer(), dataType, FORMAT_BINARY, Map.class))
            .isEqualTo(new HashMap<String, String>());
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new HStoreCodec(TEST, dataType).decode(null, dataType, FORMAT_TEXT, Map.class)).isNull();
    }

    @Test
    void canDecode() {
        HStoreCodec codec = new HStoreCodec(TEST, dataType);

        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Map.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_BINARY, Map.class)).isTrue();
        assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, Map.class)).isFalse();
        assertThat(codec.canDecode(JSON.getObjectId(), FORMAT_TEXT, Map.class)).isFalse();
        assertThat(codec.canDecode(JSONB.getObjectId(), FORMAT_BINARY, Map.class)).isFalse();
    }

    @Test
    void canDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new HStoreCodec(TEST, dataType).canDecode(dataType, null, Map.class))
            .withMessage("format must not be null");
    }

    @Test
    void canDecodeNoClass() {
        assertThatIllegalArgumentException().isThrownBy(() -> new HStoreCodec(TEST, dataType).canDecode(dataType, FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void encode() {
        ByteBuf buffer = TEST.buffer(4);
        buffer.writeInt(0);
        ParameterAssert.assertThat(new HStoreCodec(TEST, dataType).encode(new HashMap<>()))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(buffer);
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new HStoreCodec(TEST, dataType).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new HStoreCodec(TEST, dataType).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
