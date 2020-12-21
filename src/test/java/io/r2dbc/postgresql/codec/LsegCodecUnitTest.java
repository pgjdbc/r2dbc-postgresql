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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BOX;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LSEG;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LsegCodec}.
 */
final class LsegCodecUnitTest {

    private static final int dataType = LSEG.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LsegCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Lseg lseg = Lseg.of(Point.of(1.11, 2.22), Point.of(3.33, 4.44));

        ByteBuf boxTextFormat = encode(TEST, "[(1.11,2.22),(3.33,4.44)]");
        assertThat(new LsegCodec(TEST).decode(boxTextFormat, dataType, FORMAT_TEXT, Lseg.class))
            .isEqualTo(lseg);

        ByteBuf boxByteFormat = TEST.buffer(32)
            .writeDouble(1.11).writeDouble(2.22)
            .writeDouble(3.33).writeDouble(4.44);
        assertThat(new LsegCodec(TEST).decode(boxByteFormat, dataType, FORMAT_BINARY, Lseg.class))
            .isEqualTo(lseg);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new LsegCodec(TEST).decode(null, dataType, FORMAT_TEXT, Lseg.class)).isNull();
    }

    @Test
    void doCanDecode() {
        LsegCodec codec = new LsegCodec(TEST);

        assertThat(codec.doCanDecode(LSEG, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(BOX, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(POINT, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LsegCodec(TEST).doCanDecode(LSEG, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LsegCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Lseg lseg = Lseg.of(Point.of(1.11, 2.22), Point.of(3.33, 4.44));

        ParameterAssert.assertThat(new LsegCodec(TEST).doEncode(lseg))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(32)
                .writeDouble(1.11).writeDouble(2.22)
                .writeDouble(3.33).writeDouble(4.44)
            );
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LsegCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new LsegCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
