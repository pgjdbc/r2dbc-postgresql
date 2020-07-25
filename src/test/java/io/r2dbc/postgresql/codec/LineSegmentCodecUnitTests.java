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
import static io.r2dbc.postgresql.type.PostgresqlObjectId.LSEG;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link LineSegmentCodec}.
 */
final class LineSegmentCodecUnitTests {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(TEST).doCanDecode(null, FORMAT_BINARY))
            .withMessage("type must not be null");
    }

    @Test
    void doCanDecode() {
        LineSegmentCodec codec = new LineSegmentCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(LSEG, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(LSEG, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doDecodeNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(TEST).doDecode(null, LSEG, FORMAT_BINARY, LineSegment.class))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void doDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(TEST).doDecode(TEST.buffer(), LSEG, FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void doDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(TEST).doDecode(TEST.buffer(), LSEG, null, LineSegment.class))
            .withMessage("format must not be null");
    }

    @Test
    void doDecode() {
        LineSegmentCodec codec = new LineSegmentCodec(TEST);
        Point point1 = Point.of(1.12, 2.12);
        Point point2 = Point.of(3.12, 4.12);
        LineSegment lineSegment = new LineSegment(point1, point2);
        ByteBuf lineSegmentAsBinary = TEST.buffer(codec.lengthInBytes())
            .writeDouble(1.12)
            .writeDouble(2.12)
            .writeDouble(3.12)
            .writeDouble(4.12);
        assertThat(codec.doDecode(lineSegmentAsBinary, LSEG, FORMAT_BINARY, LineSegment.class)).isEqualTo(lineSegment);
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineSegmentCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void doEncode() {
        LineSegmentCodec codec = new LineSegmentCodec(TEST);
        ByteBuf lineSegmentAsBinary = TEST.buffer(codec.lengthInBytes())
            .writeDouble(1.12)
            .writeDouble(2.12)
            .writeDouble(3.12)
            .writeDouble(4.12);

        ParameterAssert.assertThat(codec.doEncode(new LineSegment(Point.of(1.12, 2.12), Point.of(3.12, 4.12))))
            .hasFormat(FORMAT_BINARY)
            .hasType(LSEG.getObjectId())
            .hasValue(lineSegmentAsBinary);
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new LineSegmentCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_BINARY, LSEG.getObjectId(), NULL_VALUE));
    }

}
