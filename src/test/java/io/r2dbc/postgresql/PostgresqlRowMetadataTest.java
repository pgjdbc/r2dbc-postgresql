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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.backend.RowDescription.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlRowMetadataTest {

    private final List<PostgresqlColumnMetadata> columnMetadatas = Arrays.asList(
        new PostgresqlColumnMetadata(MockCodecs.empty(), "test-name-2", 400, (short) 300),
        new PostgresqlColumnMetadata(MockCodecs.empty(), "test-name-1", 200, (short) 100)
    );

    @Test
    void constructorNoColumnMetadata() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(null))
            .withMessage("columnMetadatas must not be null");
    }

    @Test
    void getColumnMetadataIndex() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(0))
            .isEqualTo(new PostgresqlColumnMetadata(MockCodecs.empty(), "test-name-2", 400, (short) 300));
    }

    @Test
    void getColumnMetadataInvalidIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(2))
            .withMessage("Column index 2 is larger than the number of columns 2");
    }

    @Test
    void getColumnMetadataInvalidName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata("test-name-3"))
            .withMessage("Column name 'test-name-3' does not exist in column names [test-name-2, test-name-1]");
    }

    @Test
    void getColumnMetadataName() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata("test-name-2"))
            .isEqualTo(new PostgresqlColumnMetadata(MockCodecs.empty(), "test-name-2", 400, (short) 300));
    }

    @Test
    void getColumnMetadataNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(null))
            .withMessage("name must not be null");
    }

    @Test
    void getColumnMetadatas() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadatas()).containsAll(this.columnMetadatas);
    }

    @Test
    void getColumnNames() {
        Collection<String> columnNames = new PostgresqlRowMetadata(this.columnMetadatas).getColumnNames();

        assertThat(columnNames.contains("TEST-NAME-1")).isTrue();
        assertThat(columnNames).containsExactly("test-name-2", "test-name-1");
    }

    @Test
    void getColumnNamesModify() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnNames().remove("test-name-1"));
    }

    @Test
    void toRowMetadata() {
        MockCodecs codecs = MockCodecs.builder()
            .preferredType(200, FORMAT_TEXT, String.class)
            .build();

        PostgresqlRowMetadata rowMetadata = PostgresqlRowMetadata.toRowMetadata(codecs,
            new RowDescription(Collections.singletonList(new Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500))));

        assertThat(rowMetadata.getColumnMetadatas()).hasSize(1);
    }

    @Test
    void toRowMetadataNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlRowMetadata.toRowMetadata(null,
            new RowDescription(Collections.singletonList(new Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500)))))
            .withMessage("codecs must not be null");
    }

    @Test
    void toRowMetadataNoRowDescription() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlRowMetadata.toRowMetadata(MockCodecs.empty(), null))
            .withMessage("rowDescription must not be null");
    }

}
