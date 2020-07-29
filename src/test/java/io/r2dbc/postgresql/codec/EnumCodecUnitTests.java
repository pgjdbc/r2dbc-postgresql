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
import io.r2dbc.postgresql.message.Format;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.netty.buffer.UnpooledByteBufAllocator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

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
				new EnumCodec<EnumCodecUnitTests.MyEnum>(new UnpooledByteBufAllocator(true), MyEnum.class, 1);
		
		assertThat(codec.canDecode(1, Format.FORMAT_TEXT, MyEnum.class)).isTrue();
		assertThat(codec.canDecode(1, FORMAT_BINARY, MyEnum.class)).isTrue();
		assertThat(codec.canDecode(1, FORMAT_BINARY, Object.class)).isTrue();
		assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, MyEnum.class)).isFalse();
		assertThat(codec.canDecode(JSON.getObjectId(), FORMAT_TEXT, MyEnum.class)).isFalse();
		assertThat(codec.canDecode(JSONB.getObjectId(), FORMAT_BINARY, MyEnum.class)).isFalse();
    }
    
    @Test
    void canDecodeNoClass() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnumCodec<EnumCodecUnitTests.MyEnum>(
        		new UnpooledByteBufAllocator(true), MyEnum.class, 1).canDecode(1, Format.FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    enum MyEnum {
        INSTANCE;
    }

    enum MyOtherEnum {
        INSTANCE;
    }

}
