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
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CIRCLE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link CircleCodec}.
 */
final class CircleCodecUnitTests {

    private static final int dataType = CIRCLE.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(TEST).doCanDecode(null, FORMAT_BINARY))
            .withMessage("type must not be null");
    }

    @Test
    void doCanDecode() {
        CircleCodec codec = new CircleCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(CIRCLE, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(CIRCLE, FORMAT_BINARY)).isTrue();
    }

    @Test
    void doDecodeNoByteBuf() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(TEST).doDecode(null, CIRCLE, FORMAT_BINARY, Circle.class))
            .withMessage("byteBuf must not be null");
    }

    @Test
    void doDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(TEST).doDecode(TEST.buffer(), CIRCLE, FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void doDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(TEST).doDecode(TEST.buffer(), CIRCLE, null, Circle.class))
            .withMessage("format must not be null");
    }

    @Test
    void doDecode() {
        CircleCodec codec = new CircleCodec(TEST);
        Point point = Point.of(1.12, 2.12);
        Circle circle = Circle.of(point, 3.12);
        ByteBuf circleAsBinary = TEST.buffer(codec.lengthInBytes()).writeDouble(1.12).writeDouble(2.12).writeDouble(3.12);
        assertThat(codec.doDecode(circleAsBinary, CIRCLE, FORMAT_BINARY, Circle.class)).isEqualTo(circle);
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CircleCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void doEncode() {
        CircleCodec codec = new CircleCodec(TEST);
        ByteBuf circleAsBinary = TEST.buffer(codec.lengthInBytes()).writeDouble(1.12).writeDouble(2.12).writeDouble(3.12);

        ParameterAssert.assertThat(codec.doEncode(Circle.of(Point.of(1.12, 2.12), 3.12)))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(circleAsBinary);
    }

    @Test
    void decodeText() {
        CircleCodec codec = new CircleCodec(TEST);

        // Circles are represented by a center point and radius.
        // Values of type circle are specified using any of the following syntaxes:
        // < ( x , y ) , r >
        // ( ( x , y ) , r )
        //   ( x , y ) , r
        //     x , y   , r
        assertThat(codec.decode(encode(TEST, "<(1.2,123.1),10>"), dataType, FORMAT_TEXT, Circle.class))
            .isEqualTo(Circle.of(Point.of(1.2, 123.1), 10));

        assertThat(codec.decode(encode(TEST, "((1.2,123.1),10)"), dataType, FORMAT_TEXT, Circle.class))
            .isEqualTo(Circle.of(Point.of(1.2, 123.1), 10));

        assertThat(codec.decode(encode(TEST, "(1.2,123.1),10"), dataType, FORMAT_TEXT, Circle.class))
            .isEqualTo(Circle.of(Point.of(1.2, 123.1), 10));

        assertThat(codec.decode(encode(TEST, "1.2,123.1,10"), dataType, FORMAT_TEXT, Circle.class))
            .isEqualTo(Circle.of(Point.of(1.2, 123.1), 10));
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new CircleCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
