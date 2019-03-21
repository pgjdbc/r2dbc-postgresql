/*
 * Copyright 2017-2019 the original author or authors.
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BPCHAR_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class StringArrayCodecTest {

    @Test
    void decodeItem() {
        assertThat(new StringArrayCodec(TEST).decode(ByteBufUtils.encode(TEST, "{alpha,bravo}"), FORMAT_TEXT, String[].class))
            .isEqualTo(new String[]{"alpha", "bravo"});
    }

    @Test
    void decodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).decodeItem(null, FORMAT_TEXT, null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void decodeMultidimensional() {
        StringArrayCodec codec = new StringArrayCodec(TEST);

        assertThatIllegalArgumentException().isThrownBy(() -> codec.decode(ByteBufUtils.encode(TEST, "{{alpha},{bravo}}"), FORMAT_TEXT, Integer[][].class))
            .withMessage("type must be an array with one dimension");
    }

    @Test
    void doCanDecode() {
        StringArrayCodec codec = new StringArrayCodec(TEST);

        assertThat(codec.doCanDecode(FORMAT_TEXT, BPCHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, BPCHAR_ARRAY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, CHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, CHAR_ARRAY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, CHAR_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_TEXT, TEXT)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, TEXT_ARRAY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, TEXT_ARRAY)).isTrue();
        assertThat(codec.doCanDecode(FORMAT_TEXT, VARCHAR)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_BINARY, VARCHAR_ARRAY)).isFalse();
        assertThat(codec.doCanDecode(FORMAT_TEXT, VARCHAR_ARRAY)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).doCanDecode(null, VARCHAR_ARRAY))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).doCanDecode(FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeArray() {
        assertThat(new StringArrayCodec(TEST).encodeArray(ByteBufUtils.encode(TEST, "{alpha,bravo}")))
            .isEqualTo(new Parameter(FORMAT_TEXT, TEXT_ARRAY.getObjectId(), ByteBufUtils.encode(TEST, "{alpha,bravo}")));

    }

    @Test
    void encodeArrayNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).encodeArray(null))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItem() {
        ByteBuf actual = TEST.buffer(3);

        new StringArrayCodec(TEST).encodeItem(actual, "alpha");

        assertThat(actual).isEqualTo(ByteBufUtils.encode(TEST, "alpha"));
    }

    @Test
    void encodeItemNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).encodeItem(null, "alpha"))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StringArrayCodec(TEST).encodeItem(TEST.buffer(0), null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new StringArrayCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TEXT_ARRAY.getObjectId(), null));
    }

}