/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.PostgresqlBindingException;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class BindingTest {

    @Test
    void addNoIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(null, new Parameter(FORMAT_TEXT, 100, null)))
            .withMessage("index must not be null");
    }

    @Test
    void addNoParameter() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Binding().add(1, null))
            .withMessage("parameter must not be null");
    }

    @Test
    void empty() {
        Binding binding = new Binding();

        assertThat(binding.isEmpty()).isTrue();
    }

    @Test
    void getParameterFormats() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));
        binding.add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));

        assertThat(binding.getParameterFormats()).containsExactly(FORMAT_BINARY, FORMAT_TEXT, FORMAT_BINARY);
    }

    @Test
    void getParameterFormatsUnbound() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThatExceptionOfType(PostgresqlBindingException.class).isThrownBy(binding::getParameterFormats).withMessage("No parameter specified for index 0");
    }

    @Test
    void getParameterTypes() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));
        binding.add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));

        assertThat(binding.getParameterTypes()).containsExactly(100, VARCHAR.getObjectId(), 100);
    }

    @Test
    void getParameterTypesUnbound() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThatExceptionOfType(PostgresqlBindingException.class).isThrownBy(binding::getParameterTypes).withMessage("No parameter specified for index 0");
    }

    @Test
    void getParameterValues() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));
        binding.add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(200)));
        binding.add(1, new Parameter(FORMAT_TEXT, VARCHAR.getObjectId(), TEST.buffer().writeBytes("Hello".getBytes())));

        assertThat(binding.getParameterValues()).containsExactly(TEST.buffer(4).writeInt(200), TEST.buffer().writeBytes("Hello".getBytes()), TEST.buffer(4).writeInt(300));
    }

    @Test
    void getParameterValuesUnbound() {
        Binding binding = new Binding();
        binding.add(2, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(300)));

        assertThatExceptionOfType(PostgresqlBindingException.class).isThrownBy(binding::getParameterValues).withMessage("No parameter specified for index 0");
    }

}
