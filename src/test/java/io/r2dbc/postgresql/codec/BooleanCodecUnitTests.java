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

import java.util.Arrays;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link BooleanCodec}.
 */
final class BooleanCodecUnitTests {

    private static final int dataType = BOOL.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        Arrays.asList("1", "True", "T", "Yes", "Y", "On")
            .forEach(input -> assertThat(codec.decode(encode(TEST, input), dataType, FORMAT_TEXT, Boolean.class)).isTrue());

        Arrays.asList("0", "False", "F", "No", "N", "Off")
            .forEach(input -> assertThat(codec.decode(encode(TEST, input), dataType, FORMAT_TEXT, Boolean.class)).isFalse());
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BooleanCodec(TEST).decode(null, dataType, FORMAT_TEXT, Boolean.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        assertThat(codec.doCanDecode(BOOL, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(BOOL, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        assertThat(codec.doEncode(true))
            .hasFormat(FORMAT_TEXT)
            .hasType(BOOL.getObjectId())
            .hasValue(encode(TEST, "t"));

        assertThat(codec.doEncode(false))
            .hasFormat(FORMAT_TEXT)
            .hasType(BOOL.getObjectId())
            .hasValue(encode(TEST, "f"));
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec(TEST).doEncodeText(null))
            .withMessage("value must not be null");
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BooleanCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BooleanCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, BOOL.getObjectId(), NULL_VALUE));
    }

}
