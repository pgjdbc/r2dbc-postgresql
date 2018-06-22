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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.message.backend.Field;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class PostgresqlServerErrorExceptionTest {

    @Test
    void initializesSqlState() {

        List<Field> fields = Collections.singletonList(new Field(Field.FieldType.CODE, "1234"));

        PostgresqlServerErrorException exception = new PostgresqlServerErrorException(fields);

        assertThat(exception.getSqlState()).isEqualTo("1234");
    }

    @Test
    void initializesReason() {

        List<Field> fields = Collections.singletonList(new Field(Field.FieldType.MESSAGE, "Duplicate"));

        PostgresqlServerErrorException exception = new PostgresqlServerErrorException(fields);

        assertThat(exception.getMessage()).isEqualTo("Duplicate");
    }

    @Test
    void skipsInitializationWithEmptyFields() {

        PostgresqlServerErrorException exception = new PostgresqlServerErrorException(Collections.emptyList());

        assertThat(exception.getSqlState()).isNull();
        assertThat(exception.getMessage()).isNull();
    }
}
