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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.api.MockPostgresqlConnection;
import io.r2dbc.postgresql.api.MockPostgresqlResult;
import io.r2dbc.postgresql.api.MockPostgresqlStatement;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.TestByteBufAllocator;
import io.r2dbc.spi.test.MockRow;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.codec.EnumCodec.Builder.RegistrationPriority;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link EnumCodec}.
 */
final class EnumCodecUnitTests {

    @Test
    void shouldRejectMultipleMappingForJavaType() {
        EnumCodec.Builder builder = EnumCodec.builder().withEnum("foo", MyEnum.class);

        Assertions.assertThatIllegalArgumentException().isThrownBy(() -> builder.withEnum("bar", MyEnum.class));
    }

    @Test
    void shouldRejectMultipleMappingForTypeName() {
        EnumCodec.Builder builder = EnumCodec.builder().withEnum("foo", MyEnum.class);

        Assertions.assertThatIllegalArgumentException().isThrownBy(() -> builder.withEnum("foo", MyOtherEnum.class));
    }

    @Test
    void canDecode() {
        EnumCodec<EnumCodecUnitTests.MyEnum> codec =
            new EnumCodec<>(TestByteBufAllocator.TEST, MyEnum.class, 1);

        assertThat(codec.canDecode(1, Format.FORMAT_TEXT, MyEnum.class)).isTrue();
        assertThat(codec.canDecode(1, FORMAT_BINARY, MyEnum.class)).isTrue();
        assertThat(codec.canDecode(1, FORMAT_BINARY, Object.class)).isTrue();
        assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, MyEnum.class)).isFalse();
        assertThat(codec.canDecode(JSON.getObjectId(), FORMAT_TEXT, MyEnum.class)).isFalse();
        assertThat(codec.canDecode(JSONB.getObjectId(), FORMAT_BINARY, MyEnum.class)).isFalse();
    }

    @Test
    void canDecodeWithoutClassShouldThrowException() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec<>(
            TestByteBufAllocator.TEST, MyEnum.class, 1).canDecode(1, Format.FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void shouldRegisterCodecAsFirst() {
        CodecRegistrar codecRegistrar = EnumCodec
            .builder()
            .withRegistrationPriority(RegistrationPriority.FIRST)
            .withEnum("foo", MyEnum.class)
            .build();

        ByteBufAllocator mockByteBufAllocator = mock(ByteBufAllocator.class);
        CodecRegistry mockCodecRegistry = mock(CodecRegistry.class);

        MockPostgresqlStatement mockPostgresqlStatement = MockPostgresqlStatement.builder()
            .result(MockPostgresqlResult.builder()
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 42)
                    .identified("typarray", Integer.class, 0)
                    .identified("typname", String.class, "foo")
                    .identified("typcategory", String.class, "E")
                    .build())
                .build())
            .build();
        MockPostgresqlConnection mockPostgresqlConnection = new MockPostgresqlConnection(mockPostgresqlStatement);

        Publisher<Void> register = codecRegistrar.register(mockPostgresqlConnection, mockByteBufAllocator, mockCodecRegistry);
        StepVerifier.create(register).verifyComplete();

        verify(mockCodecRegistry, only()).addFirst(any(EnumCodec.class));
        verify(mockCodecRegistry, never()).addLast(any(EnumCodec.class));
    }

    @Test
    void shouldRegisterCodecAsLast() {
        CodecRegistrar codecRegistrar = EnumCodec
            .builder()
            .withRegistrationPriority(RegistrationPriority.LAST)
            .withEnum("foo", MyEnum.class)
            .withEnum("bar", MyOtherEnum.class)
            .build();

        ByteBufAllocator mockByteBufAllocator = mock(ByteBufAllocator.class);
        CodecRegistry mockCodecRegistry = mock(CodecRegistry.class);

        MockPostgresqlStatement mockPostgresqlStatement = MockPostgresqlStatement.builder()
            .result(MockPostgresqlResult.builder()
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 42)
                    .identified("typarray", Integer.class, 0)
                    .identified("typname", String.class, "foo")
                    .identified("typcategory", String.class, "E")
                    .build())
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 43)
                    .identified("typarray", Integer.class, 0)
                    .identified("typname", String.class, "bar")
                    .identified("typcategory", String.class, "E")
                    .build())
                .build())
            .build();
        MockPostgresqlConnection mockPostgresqlConnection = new MockPostgresqlConnection(mockPostgresqlStatement);

        Publisher<Void> register = codecRegistrar.register(mockPostgresqlConnection, mockByteBufAllocator, mockCodecRegistry);
        StepVerifier.create(register).verifyComplete();

        verify(mockCodecRegistry, never()).addFirst(any(EnumCodec.class));
        verify(mockCodecRegistry, times(2)).addLast(any(EnumCodec.class));
    }

    enum MyEnum {
        INSTANCE;
    }

    enum MyOtherEnum {
        INSTANCE;
    }

}
