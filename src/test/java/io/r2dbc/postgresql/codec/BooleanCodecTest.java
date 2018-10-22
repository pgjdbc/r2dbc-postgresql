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

import java.util.Arrays;

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BOOL;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class BooleanCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new BooleanCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        Arrays.asList("1", "True", "T", "Yes", "Y", "On")
            .forEach(input -> assertThat(codec.decode(encode(TEST, input), TEXT, Boolean.class)).isTrue());

        Arrays.asList("0", "False", "F", "No", "N", "Off")
            .forEach(input -> assertThat(codec.decode(encode(TEST, input), TEXT, Boolean.class)).isFalse());
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BooleanCodec(TEST).decode(null, TEXT, Boolean.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        assertThat(codec.doCanDecode(BINARY, BOOL)).isFalse();
        assertThat(codec.doCanDecode(TEXT, MONEY)).isFalse();
        assertThat(codec.doCanDecode(TEXT, BOOL)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new BooleanCodec(TEST).doCanDecode(null, VARCHAR))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new BooleanCodec(TEST).doCanDecode(TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        BooleanCodec codec = new BooleanCodec(TEST);

        assertThat(codec.doEncode(true))
            .isEqualTo(new Parameter(TEXT, BOOL.getObjectId(), encode(TEST, "TRUE")));

        assertThat(codec.doEncode(false))
            .isEqualTo(new Parameter(TEXT, BOOL.getObjectId(), encode(TEST, "FALSE")));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new BooleanCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new BooleanCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(TEXT, BOOL.getObjectId(), null));
    }

}
