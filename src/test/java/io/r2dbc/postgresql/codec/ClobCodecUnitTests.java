/*
 * Copyright 2017 the original author or authors.
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
import io.r2dbc.postgresql.client.ParameterAssert;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.test.MockClob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ClobCodec}.
 */
final class ClobCodecUnitTests {

    private static final int dataType = TEXT.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ClobCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Flux.from(new ClobCodec(TEST).decode(encode(TEST, "test"), dataType, FORMAT_TEXT, Clob.class).stream())
            .reduce(new StringBuilder(), StringBuilder::append)
            .map(StringBuilder::toString)
            .as(StepVerifier::create)
            .expectNext("test")
            .verifyComplete();
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new ClobCodec(TEST).decode(null, dataType, FORMAT_TEXT, Clob.class)).isNull();
    }

    @Test
    void doCanDecode() {
        ClobCodec codec = new ClobCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(TEXT, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ClobCodec(TEST).doCanDecode(TEXT, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ClobCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        MockClob clob = MockClob.builder()
            .item("test", "-", "value")
            .build();

        ParameterAssert.assertThat(new ClobCodec(TEST).doEncode(clob))
            .hasFormat(FORMAT_TEXT)
            .hasType(VARCHAR.getObjectId())
            .hasValue(encode(TEST, "test-value"));

        assertThat(clob.isDiscardCalled()).isTrue();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ClobCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new ClobCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, TEXT.getObjectId(), NULL_VALUE));
    }

}
