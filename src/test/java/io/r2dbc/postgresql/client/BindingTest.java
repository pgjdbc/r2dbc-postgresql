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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.PostgresqlBindingException;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.fail;

final class BindingTest {

    @Test
    void addNoIndex() {
        assertThatNullPointerException().isThrownBy(() -> new Binding().add(null, new Parameter(TEXT, 100, null)))
            .withMessage("index must not be null");
    }

    @Test
    void addNoParameter() {
        assertThatNullPointerException().isThrownBy(() -> new Binding().add(1, null))
            .withMessage("parameter must not be null");
    }

    @Test
    void empty() {
        Binding binding = new Binding();

        assertThat(binding.isEmpty());
    }

    @Test
    void getParameterFormats() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));

        try {
            binding.getParameterFormats();
        } catch (PostgresqlBindingException ex ){
            assertThat(ex.getMessage().equals("Null parameter found"));
        }
        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));
        try {
            assertThat(binding.getParameterFormats()).containsExactly(BINARY, TEXT, BINARY);
        } catch (PostgresqlBindingException ex ){
            fail("should not get this exception");
        }
    }

    @Test
    void getParameterTypes() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));
        try {
            binding.getParameterTypes();
        }catch (PostgresqlBindingException ex ){
            assertThat(ex.getMessage().equals("Null parameter found"));
        }
        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));

        try {
            assertThat(binding.getParameterTypes()).containsExactly(100, VARCHAR.getObjectId(), 100);
        } catch (PostgresqlBindingException ex ){
            fail("should not get this exception");
        }
    }

    @Test
    void getParameterValues() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(300)));
        try {
            binding.getParameterTypes();
        }catch (PostgresqlBindingException ex ){
            assertThat(ex.getMessage().equals("Null parameter found"));
        }

        binding.add(0, new Parameter(BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));

        try {
            assertThat(binding.getParameterTypes()).containsExactly(100, VARCHAR.getObjectId(), 100);
            assertThat(binding.getParameterValues()).containsExactly(TEST.buffer(4).writeInt(200), TEST.buffer().writeBytes("Hello".getBytes()), TEST.buffer(4).writeInt(300));
        } catch (PostgresqlBindingException ex ){
            fail("should not get this exception");
        }
    }

}
