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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.message.backend.Field.FieldType;
import org.junit.jupiter.api.Test;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.COLUMN_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CONSTRAINT_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.DATA_TYPE_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.DETAIL;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.FILE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.HINT;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.INTERNAL_POSITION;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.INTERNAL_QUERY;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.LINE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.MESSAGE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.POSITION;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.ROUTINE;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SCHEMA_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SEVERITY_LOCALIZED;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.SEVERITY_NON_LOCALIZED;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.TABLE_NAME;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.UNKNOWN;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.WHERE;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class FieldTest {

    @Test
    void constructorNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Field(null, "test-value"))
            .withMessage("type must not be null");
    }

    @Test
    void constructorNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Field(CODE, null))
            .withMessage("value must not be null");
    }

    @Test
    void decode() {
        ByteBuf buffer = TEST.buffer()
            .writeByte('C');

        buffer.writeCharSequence("test-value", UTF_8);
        buffer.writeByte(0);
        buffer.writeByte(0);

        assertThat(Field.decode(buffer)).containsExactly(new Field(CODE, "test-value"));
    }

    @Test
    void valueOfCode() {
        assertThat(FieldType.valueOf((byte) 'C')).isEqualTo(CODE);
    }

    @Test
    void valueOfColumnName() {
        assertThat(FieldType.valueOf((byte) 'c')).isEqualTo(COLUMN_NAME);
    }

    @Test
    void valueOfConstraintName() {
        assertThat(FieldType.valueOf((byte) 'n')).isEqualTo(CONSTRAINT_NAME);
    }

    @Test
    void valueOfDataTypeName() {
        assertThat(FieldType.valueOf((byte) 'd')).isEqualTo(DATA_TYPE_NAME);
    }

    @Test
    void valueOfDetail() {
        assertThat(FieldType.valueOf((byte) 'D')).isEqualTo(DETAIL);
    }

    @Test
    void valueOfFile() {
        assertThat(FieldType.valueOf((byte) 'F')).isEqualTo(FILE);
    }

    @Test
    void valueOfHint() {
        assertThat(FieldType.valueOf((byte) 'H')).isEqualTo(HINT);
    }

    @Test
    void valueOfInternalPosition() {
        assertThat(FieldType.valueOf((byte) 'p')).isEqualTo(INTERNAL_POSITION);
    }

    @Test
    void valueOfInternalQuery() {
        assertThat(FieldType.valueOf((byte) 'q')).isEqualTo(INTERNAL_QUERY);
    }

    @Test
    void valueOfLine() {
        assertThat(FieldType.valueOf((byte) 'L')).isEqualTo(LINE);
    }

    @Test
    void valueOfMessage() {
        assertThat(FieldType.valueOf((byte) 'M')).isEqualTo(MESSAGE);
    }

    @Test
    void valueOfPosition() {
        assertThat(FieldType.valueOf((byte) 'P')).isEqualTo(POSITION);
    }

    @Test
    void valueOfRoutine() {
        assertThat(FieldType.valueOf((byte) 'R')).isEqualTo(ROUTINE);
    }

    @Test
    void valueOfSchemaName() {
        assertThat(FieldType.valueOf((byte) 's')).isEqualTo(SCHEMA_NAME);
    }

    @Test
    void valueOfSeverityLocalized() {
        assertThat(FieldType.valueOf((byte) 'S')).isEqualTo(SEVERITY_LOCALIZED);
    }

    @Test
    void valueOfSeverityNonLocalized() {
        assertThat(FieldType.valueOf((byte) 'V')).isEqualTo(SEVERITY_NON_LOCALIZED);
    }

    @Test
    void valueOfTableName() {
        assertThat(FieldType.valueOf((byte) 't')).isEqualTo(TABLE_NAME);
    }

    @Test
    void valueOfUnknown() {
        assertThat(FieldType.valueOf((byte) 'X')).isEqualTo(UNKNOWN);
    }

    @Test
    void valueOfWhere() {
        assertThat(FieldType.valueOf((byte) 'W')).isEqualTo(WHERE);
    }

}
