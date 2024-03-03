/*
 * Copyright 2017 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.util.ReferenceCountedCleaner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PostgresqlRow}.
 */
final class PostgresqlRowUnitTests {

    private final ReferenceCountedCleaner cleaner = new ReferenceCountedCleaner();

    private final List<RowDescription.Field> columns = Arrays.asList(
        new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_BINARY, "test-name-1", 500),
        new RowDescription.Field((short) 300, 400, 300, (short) 400, FORMAT_TEXT, "test-name-2", 500),
        new RowDescription.Field((short) 400, 400, 300, (short) 400, FORMAT_TEXT, "test-name-3", 500)
    );

    private final PostgresqlRowMetadata metadata = new PostgresqlRowMetadata(columns
        .stream()
        .map(field -> PostgresqlColumnMetadata.toColumnMetadata(mock(Codecs.class), field))
        .collect(Collectors.toList()));

    private final ByteBuf[] data = new ByteBuf[]{TEST.buffer(4).writeInt(100), TEST.buffer(4).writeInt(300), null};

    @AfterEach
    void tearDown() {
        cleaner.clean();
    }

    @Test
    void constructorNoContext() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRow(null, null, Collections.emptyList(), null))
            .withMessage("context must not be null");
    }

    @Test
    void constructorNoColumns() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRow(MockContext.empty(), mock(io.r2dbc.postgresql.api.PostgresqlRowMetadata.class), null, null))
            .withMessage("fields must not be null");
    }

    @Test
    void getAfterRelease() {
        Object value = new Object();

        MockCodecs codecs = MockCodecs.builder()
            .decoding(TEST.buffer(4).writeInt(300), 400, FORMAT_TEXT, Object.class, value)
            .build();

        PostgresqlRow row = new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, new ByteBuf[0]);
        row.release();

        assertThatIllegalStateException().isThrownBy(() -> row.get("test-name-2", Object.class))
            .withMessage("Value cannot be retrieved after row has been released");
    }

    @Test
    void getDefaultType() {
        Object value = new Object();

        MockCodecs codecs = MockCodecs.builder()
            .decoding(TEST.buffer(4).writeInt(300), 400, FORMAT_TEXT, Object.class, value)
            .build();

        assertThat(new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, this.data).get("test-name-2")).isSameAs(value);
    }

    @Test
    void getIndex() {
        Object value = new Object();

        MockCodecs codecs = MockCodecs.builder()
            .decoding(TEST.buffer(4).writeInt(300), 400, FORMAT_TEXT, Object.class, value)
            .build();

        assertThat(new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, this.data).get(1, Object.class)).isSameAs(value);
    }

    @Test
    void getInvalidIndex() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class).isThrownBy(() -> new PostgresqlRow(MockContext.empty(), this.metadata, this.columns,
                new ByteBuf[0]).get(3,
                Object.class))
            .withMessage("Column index 3 is larger than the number of columns 3");
    }

    @Test
    void getInvalidName() {
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> new PostgresqlRow(MockContext.empty(), this.metadata, this.columns,
                new ByteBuf[0]).get("test-name" +
                "-4", Object.class))
            .withMessageMatching("Column name 'test-name-4' does not exist in column names \\[test-name-[\\d], test-name-[\\d], test-name-[\\d]\\]");
    }

    @Test
    void getName() {
        Object value = new Object();

        MockCodecs codecs = MockCodecs.builder()
            .decoding(TEST.buffer(4).writeInt(300), 400, FORMAT_TEXT, Object.class, value)
            .build();

        assertThat(new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, this.data).get("test-name-2", Object.class)).isSameAs(value);
        assertThat(new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, this.data).get("tEsT-nAme-2", Object.class)).isSameAs(value);
    }

    @Test
    void getNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRow(MockContext.empty(), this.metadata, this.columns, new ByteBuf[0]).get(null,
            Object.class))
            .withMessage("name must not be null");
    }

    @Test
    void getNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlRow(MockContext.empty(), this.metadata, this.columns, new ByteBuf[0]).get("", null))
            .withMessage("type must not be null");
    }

    @Test
    void getNull() {
        MockCodecs codecs = MockCodecs.builder()
            .decoding(null, 400, FORMAT_TEXT, Object.class, null)
            .build();

        assertThat(new PostgresqlRow(MockContext.builder().codecs(codecs).build(), this.metadata, this.columns, this.data).get("test-name-3", Object.class)).isNull();
    }

    @Test
    void toRow() {
        Object value = new Object();

        MockCodecs codecs = MockCodecs.builder()
            .decoding(TEST.buffer(4).writeInt(100), 300, FORMAT_TEXT, Object.class, value)
            .build();

        RowDescription description = new RowDescription(Collections.singletonList(new RowDescription.Field((short) 200, 300, (short) 400, (short) 500, FORMAT_TEXT, "test-name-1", 600)));
        PostgresqlRow row = PostgresqlRow.toRow(MockContext.builder().codecs(codecs).build(), cleaner.capture(new DataRow(TEST.buffer(4).writeInt(100))),
            codecs, description);

        assertThat(row.get(0, Object.class)).isSameAs(value);
    }

    @Test
    void toRowNoDataRow() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlRow.toRow(MockContext.empty(), null, MockCodecs.empty(), new RowDescription(Collections.emptyList())))
            .withMessage("dataRow must not be null");
    }

    @Test
    void toRowNoRowDescription() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlRow.toRow(MockContext.empty(), cleaner.capture(new DataRow(TEST.buffer(4).writeInt(100))), MockCodecs.empty(), null))
            .withMessage("rowDescription must not be null");
    }

}
