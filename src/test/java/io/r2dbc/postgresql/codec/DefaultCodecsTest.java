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

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class DefaultCodecsTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), BINARY, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), BINARY, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), null, Object.class))
            .withMessage("format must not be null");
    }

    @Test
    void decodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs(TEST).decode(null, INT4.getObjectId(), BINARY, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), BINARY, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void");
    }

    @Test
    void encode() {
        Parameter parameter = new DefaultCodecs(TEST).encode(100);

        assertThat(parameter).isEqualTo(new Parameter(BINARY, INT4.getObjectId(), TEST.buffer(4).writeInt(100)));
    }

    @Test
    void encodeNull() {
        Parameter parameter = new DefaultCodecs(TEST).encode(null);

        assertThat(parameter).isEqualTo(new Parameter(BINARY, UNSPECIFIED.getObjectId(), null));
    }

    @Test
    void encodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).encode(new Object()))
            .withMessage("Cannot encode parameter of type java.lang.Object");
    }

}
