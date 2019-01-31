/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.postgresql.message.backend.RowDescription.Field;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlColumnMetadataTest {

    @Test
    void constructorNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlColumnMetadata(null, (short) 100, 200))
            .withMessage("name must not be null");
    }

    @Test
    void constructorNoPrecision() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlColumnMetadata("test-name", null, 200))
            .withMessage("precision must not be null");
    }

    @Test
    void constructorNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlColumnMetadata("test-name", (short) 100, null))
            .withMessage("type must not be null");
    }

    @Test
    void toColumnMetadata() {
        PostgresqlColumnMetadata columnMetadata = PostgresqlColumnMetadata.toColumnMetadata(new Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500));

        assertThat(columnMetadata.getName()).isEqualTo("test-name");
        assertThat(columnMetadata.getPrecision()).isEqualTo(400);
    }

    @Test
    void toColumnMetadataNoField() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlColumnMetadata.toColumnMetadata(null))
            .withMessage("field must not be null");
    }

}
