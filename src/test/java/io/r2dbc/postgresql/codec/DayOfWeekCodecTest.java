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

import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.DayOfWeek;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NUMERIC;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class DayOfWeekCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(null))
                .withMessage("byteBufAllocator must not be null");
    }

    @ParameterizedTest
    @EnumSource(DayOfWeek.class)
    void decode(DayOfWeek d) {
        assertThat(new DayOfWeekCodec(TEST).decode(TEST.buffer().writeInt(d.getValue()), INT4, FORMAT_BINARY, DayOfWeek.class)).isEqualTo(d);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new DayOfWeekCodec(TEST).decode(null, INT4.getObjectId(), FORMAT_BINARY, DayOfWeek.class)).isNull();
    }

    @Test
    void doCanDecode() {
        DayOfWeekCodec codec = new DayOfWeekCodec(TEST);

        assertThat(codec.doCanDecode(INT4, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT2, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(INT8, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(NUMERIC, FORMAT_TEXT)).isTrue();
        assertThat(codec.doCanDecode(VARCHAR, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).doCanDecode(null, FORMAT_BINARY))
                .withMessage("type must not be null");
    }

    @ParameterizedTest
    @EnumSource(DayOfWeek.class)
    void doEncodeInt(DayOfWeek d) {
        ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT4.getObjectId())
            .hasValue(TEST.buffer().writeInt(d.getValue()));
    }

    @ParameterizedTest
    @EnumSource(DayOfWeek.class)
    void doEncodeShort(DayOfWeek d) {
        ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d, INT2))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT2.getObjectId())
            .hasValue(TEST.buffer().writeShort(d.getValue()));
    }

    @ParameterizedTest
    @EnumSource(DayOfWeek.class)
    void doEncodeLong(DayOfWeek d) {
        ParameterAssert.assertThat(new DayOfWeekCodec(TEST).doEncode(d, INT8))
            .hasFormat(FORMAT_BINARY)
            .hasType(INT8.getObjectId())
            .hasValue(TEST.buffer().writeLong(d.getValue()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).doEncode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeItemNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DayOfWeekCodec(TEST).encode(null))
                .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new DayOfWeekCodec(TEST).encodeNull())
                .isEqualTo(new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE));
    }

}
