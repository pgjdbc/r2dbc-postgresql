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

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.LINE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.PATH;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.POLYGON;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PolygonCodec}.
 */
final class PolygonCodecUnitTest {

    private static final int dataType = POLYGON.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PolygonCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Polygon polygon = Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14));

        assertThat(new PolygonCodec(TEST).decode(encode(TEST, "((-10.42,3.14),(10.42,-3.14))"), dataType, FORMAT_TEXT, Polygon.class)).isEqualTo(polygon);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new PolygonCodec(TEST).decode(null, dataType, FORMAT_TEXT, Polygon.class)).isNull();
    }

    @Test
    void doCanDecode() {
        PolygonCodec codec = new PolygonCodec(TEST);
        assertThat(codec.doCanDecode(POLYGON, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(LINE, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(PATH, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PolygonCodec(TEST).doCanDecode(PATH, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PolygonCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        Polygon polygon = Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14));
        ParameterAssert.assertThat(new PolygonCodec(TEST).doEncode(polygon))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(TEST.buffer(37)
                .writeInt(2)
                .writeDouble(-10.42)
                .writeDouble(3.14)
                .writeDouble(10.42)
                .writeDouble(-3.14)
            );
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PolygonCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void decodeText() {
        // Polygons are represented by lists of points (the vertexes of the polygon).
        // Values of type polygon are specified using any of the following syntaxes:
        //  ( ( x1 , y1 ) , ... , ( xn , yn ) )
        //    ( x1 , y1 ) , ... , ( xn , yn )
        //    ( x1 , y1   , ... ,   xn , yn )
        //      x1 , y1   , ... ,   xn , yn
        PolygonCodec codec = new PolygonCodec(TEST);

        assertThat(codec.decode(encode(TEST, "((-10.42,3.14),(10.42,-3.14))"), dataType, FORMAT_TEXT, Polygon.class))
            .isEqualTo(Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)));
        assertThat(codec.decode(encode(TEST, "(-10.42,3.14),(10.42,-3.14)"), dataType, FORMAT_TEXT, Polygon.class))
            .isEqualTo(Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)));
        assertThat(codec.decode(encode(TEST, "(-10.42,3.14,10.42,-3.14)"), dataType, FORMAT_TEXT, Polygon.class))
            .isEqualTo(Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)));
        assertThat(codec.decode(encode(TEST, "-10.42,3.14,10.42,-3.14"), dataType, FORMAT_TEXT, Polygon.class))
            .isEqualTo(Polygon.of(Point.of(-10.42, 3.14), Point.of(10.42, -3.14)));
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new PolygonCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
