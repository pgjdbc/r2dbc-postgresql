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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link VectorCodec}.
 */
class VectorCodecUnitTests {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new VectorCodec(null, 0,0))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void doCanDecode() {
        VectorCodec codec = new VectorCodec(TEST, 16384,0);

        assertThat(codec.canDecode(16384, FORMAT_BINARY, Object.class)).isTrue();
        assertThat(codec.canDecode(16384, FORMAT_TEXT, Object.class)).isTrue();
        assertThat(codec.canDecode(16384, FORMAT_BINARY, Vector.class)).isTrue();
        assertThat(codec.canDecode(16384, FORMAT_TEXT, Vector.class)).isTrue();
        assertThat(codec.canDecode(16384, FORMAT_BINARY, Float[].class)).isTrue();
        assertThat(codec.canDecode(16384, FORMAT_BINARY, float[].class)).isTrue();
    }

    @Test
    void decodeText() {
        VectorCodec codec = new VectorCodec(TEST, 16384,0);
        Vector vector = Vector.of(1.1f, 2.2f, 3f);
        ByteBuf vectorAsText = encode(TEST, "[1.1,2.2,3]");
        assertThat(codec.decode(vectorAsText, 16384, FORMAT_TEXT, Vector.class)).isEqualTo(vector);
    }

    @Test
    void decodeBinary() {
        VectorCodec codec = new VectorCodec(TEST, 16384,0);
        Vector vector = Vector.of(1.1f, 2.2f, 3f);
        ByteBuf vectorAsBinary = TEST.buffer(16).writeShort(3).writeShort(0).writeFloat(1.1f).writeFloat(2.2f).writeFloat(3f);
        assertThat(codec.decode(vectorAsBinary, 16384, FORMAT_BINARY, Vector.class)).isEqualTo(vector);
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new VectorCodec(TEST, 1234,0).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, 1234, NULL_VALUE));
    }

    @Test
    void encodeBinary() {
         ParameterAssert.assertThat(new VectorCodec(TEST, 1234,0).encode(Vector.of(1.1f, 2.2f, 3f)))
            .hasFormat(FORMAT_BINARY)
            .hasType(1234)
            .hasValue(TEST.buffer(16).writeShort(3).writeShort(0).writeFloat(1.1f).writeFloat(2.2f).writeFloat(3f));
    }
}
