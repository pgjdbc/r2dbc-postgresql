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

import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.test.MockBlob;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BYTEA;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link BlobCodec}.
 */
final class BlobCodecUnitTests {

    private static final int dataType = BYTEA.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BlobCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() {
        Flux.from(new BlobCodec(TEST).decode(encode(TEST, "\\\\x746573742d76616c7565"), dataType, FORMAT_TEXT, Blob.class).stream())
            .reduce(TEST.compositeBuffer(), (a, b) -> a.addComponent(true, Unpooled.wrappedBuffer(b)))
            .map(ByteBufUtils::decode)
            .as(StepVerifier::create)
            .expectNext("test-value")
            .verifyComplete();
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new BlobCodec(TEST).decode(null, dataType, FORMAT_TEXT, Blob.class)).isNull();
    }

    @Test
    void doCanDecode() {
        BlobCodec codec = new BlobCodec(TEST);

        assertThat(codec.doCanDecode(VARCHAR, FORMAT_BINARY)).isFalse();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
        assertThat(codec.doCanDecode(BYTEA, FORMAT_TEXT)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BlobCodec(TEST).doCanDecode(BYTEA, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BlobCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() {
        MockBlob Blob = MockBlob.builder()
            .item(ByteBufUtils.encode(TEST, "test").nioBuffer())
            .item(ByteBufUtils.encode(TEST, "-").nioBuffer())
            .item(ByteBufUtils.encode(TEST, "value").nioBuffer())
            .build();

        ParameterAssert.assertThat(new BlobCodec(TEST).doEncode(Blob))
            .hasFormat(FORMAT_TEXT)
            .hasType(BYTEA.getObjectId())
            .hasValue(encode(TEST, "\\x746573742d76616c7565"));

        assertThat(Blob.isDiscardCalled()).isTrue();
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new BlobCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        ParameterAssert.assertThat(new BlobCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(FORMAT_TEXT, BYTEA.getObjectId(), NULL_VALUE));
    }

}
