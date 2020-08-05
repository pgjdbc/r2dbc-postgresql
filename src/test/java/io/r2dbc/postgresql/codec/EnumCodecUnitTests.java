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
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import static io.r2dbc.postgresql.codec.EnumCodec.Builder.RegistrationPriority;
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
                .rowMetadata(MockRowMetadata.empty())
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 42)
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
                .rowMetadata(MockRowMetadata.empty())
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 42)
                    .identified("typname", String.class, "foo")
                    .identified("typcategory", String.class, "E")
                    .build())
                .row(MockRow.builder()
                    .identified("oid", Integer.class, 43)
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
