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

import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.backend.RowDescription.Field;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlRowMetadataTest {

    private final List<PostgresqlColumnMetadata> columnMetadatas = Arrays.asList(
        new PostgresqlColumnMetadata("test-name-1", (short) 100, 200),
        new PostgresqlColumnMetadata("test-name-2", (short) 300, 400)
    );

    @Test
    void constructorNoColumnMetadata() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(null))
            .withMessage("columnMetadatas must not be null");
    }

    @Test
    void getColumnMetadataIndex() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(1))
            .isEqualTo(new PostgresqlColumnMetadata("test-name-2", (short) 300, 400));
    }

    @Test
    void getColumnMetadataInvalidIndex() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(2))
            .withMessage("Column index 2 is larger than the number of columns 2");
    }

    @Test
    void getColumnMetadataInvalidName() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata("test-name-3"))
            .withMessage("Column name 'test-name-3' does not exist in column names [test-name-1, test-name-2]");
    }

    @Test
    void getColumnMetadataName() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata("test-name-2"))
            .isEqualTo(new PostgresqlColumnMetadata("test-name-2", (short) 300, 400));
    }

    @Test
    void getColumnMetadataNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(null))
            .withMessage("identifier must not be null");
    }

    @Test
    void getColumnMetadataWrongIdentifierType() {
        Object identifier = new Object();

        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadata(identifier))
            .withMessage("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier.toString());
    }

    @Test
    void getColumnMetadatas() {
        assertThat(new PostgresqlRowMetadata(this.columnMetadatas).getColumnMetadatas()).containsAll(this.columnMetadatas);
    }

    @Test
    void toRowMetadata() {
        PostgresqlRowMetadata rowMetadata = PostgresqlRowMetadata.toRowMetadata(
            new RowDescription(Collections.singletonList(new Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500))));

        assertThat(rowMetadata.getColumnMetadatas()).hasSize(1);
    }

    @Test
    void toRowMetadataNoRowDescription() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlRowMetadata.toRowMetadata(null))
            .withMessage("rowDescription must not be null");
    }

}
