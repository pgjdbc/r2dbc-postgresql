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
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMPTZ;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
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
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs(TEST).decode(TEST.buffer(4).writeInt(100), INT4.getObjectId(), FORMAT_BINARY, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), null, Object.class))
            .withMessage("format must not be null");
    }

    @Test
    void decodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).decode(TEST.buffer(4), INT4.getObjectId(), FORMAT_BINARY, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void");
    }

    @Test
    void delegatePriority() {
        Codecs codecs = new DefaultCodecs(TEST);

        assertThat(codecs.decode(TEST.buffer(2).writeShort((byte) 100), INT2.getObjectId(), FORMAT_BINARY, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "100"), INT2.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(Short.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "test"), VARCHAR.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(String.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"), TIMESTAMP.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(Instant.class);
        assertThat(codecs.decode(ByteBufUtils.encode(TEST, "2018-11-05 00:35:43.048593+09"), TIMESTAMPTZ.getObjectId(), FORMAT_TEXT, Object.class)).isInstanceOf(ZonedDateTime.class);
    }

    @Test
    void encode() {
        Parameter parameter = new DefaultCodecs(TEST).encode(100);

        assertThat(parameter).isEqualTo(new Parameter(FORMAT_BINARY, INT4.getObjectId(), TEST.buffer(4).writeInt(100)));
    }

    @Test
    void encodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        Parameter parameter = new DefaultCodecs(TEST).encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(new Parameter(FORMAT_BINARY, INT4.getObjectId(), null));
    }

    @Test
    void encodeNullNoType() {
        assertThatNullPointerException().isThrownBy(() -> new DefaultCodecs(TEST).encodeNull(null))
            .withMessage("type must not be null");
    }

    //@Test
    void encodeNullUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).encodeNull(Object.class))
            .withMessage("Cannot encode null parameter of type java.lang.Object");
    }

    //@Test
    void encodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(TEST).encode(new Object()))
            .withMessage("Cannot encode parameter of type java.lang.Object");
    }
}
