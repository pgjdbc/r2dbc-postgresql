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
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.LINE;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LineCodec}.
 */
final class LineCodecUnitTest {

    private static final int dataType = LINE.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Line line = Line.of(-10.42, 3.14, 5.24);

        ByteBuf lineTextFormat = encode(TEST, "{-10.42,3.14,5.24}");
        assertThat(new LineCodec(TEST).decode(lineTextFormat, dataType, FORMAT_TEXT, Line.class))
            .isEqualTo(line);

        ByteBuf lineByteFormat = TEST.buffer(24)
            .writeDouble(-10.42).writeDouble(3.14).writeDouble(5.24);
        assertThat(new LineCodec(TEST).decode(lineByteFormat, dataType, FORMAT_BINARY, Line.class))
            .isEqualTo(line);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LineCodec(TEST).decode(null, dataType, FORMAT_TEXT, Line.class)).isNull();
    }

    @Test
    void doCanDecode() {
        LineCodec codec = new LineCodec(TEST);

        assertThat(codec.doCanDecode(LINE, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(POINT, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineCodec(TEST).doCanDecode(LINE, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Line line = Line.of(-10.42, 3.14, 5.24);

        ParameterAssert.assertThat(new LineCodec(TEST).doEncode(line))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(24)
                .writeDouble(-10.42)
                .writeDouble(3.14)
                .writeDouble(5.24)
            );
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new LineCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
