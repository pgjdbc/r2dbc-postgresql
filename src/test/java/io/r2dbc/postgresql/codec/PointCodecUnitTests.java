/*
 * Copyright 2017-2020 the original author or authors.
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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.POINT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PointCodec}.
 */
final class PointCodecUnitTests {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(TEST).doCanDecode(null, FORMAT_BINARY))
            .withMessage("type must not be null");
    }

    @Test
    void doCanDecode() {
        PointCodec codec = new PointCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(POINT, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(POINT, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doDecodeNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(TEST).doDecode(null, POINT, FORMAT_BINARY, Point.class))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void doDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(TEST).doDecode(TEST.buffer(), POINT, FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void doDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(TEST).doDecode(TEST.buffer(), POINT, null, Point.class))
            .withMessage("format must not be null");
    }

    @Test
    void doDecode() {
        PointCodec codec = new PointCodec(TEST);
        Point point = Point.of(1.12, 2.12);
        ByteBuf pointAsBinary = TEST.buffer(codec.lengthInBytes()).writeDouble(1.12).writeDouble(2.12);
        assertThat(codec.doDecode(pointAsBinary, POINT, FORMAT_BINARY, Point.class)).isEqualTo(point);
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PointCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void doEncode() {
        PointCodec codec = new PointCodec(TEST);
        ByteBuf pointAsBinary = TEST.buffer(codec.lengthInBytes()).writeDouble(1.12).writeDouble(2.12);

        ParameterAssert.assertThat(codec.doEncode(Point.of(1.12, 2.12)))
            .hasFormat(FORMAT_BINARY)
            .hasType(POINT.getObjectId())
            .hasValue(pointAsBinary);
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new PointCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, POINT.getObjectId(), NULL_VALUE));
    }

}
