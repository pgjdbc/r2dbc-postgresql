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

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.Query;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

final class SimpleQueryPostgresqlStatementTest {

    @Test
    void bind() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "test-query").bind(null, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }

    @Test
    void bindIndex() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "test-query").bind(0, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }


    @Test
    void bindNull() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "test-query").bindNull(null, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }

    @Test
    void bindNullIndex() {
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "test-query").bindNull(0, null))
            .withMessage("Binding parameters is not supported for the statement 'test-query'");
    }


    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(null, MockCodecs.empty(), "test-query"))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, null, "test-query"))
            .withMessage("codecs must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), null))
            .withMessage("sql must not be null");
    }

    @Test
    void executeCommandCompleteMap() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeCommandCompleteRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void executeEmptyQueryResponseRows() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(EmptyQueryResponse.INSTANCE)
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeEmptyQueryResponseRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(EmptyQueryResponse.INSTANCE)
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void executeErrorResponseRows() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyError(PostgresqlServerErrorException.class);
    }

    @Test
    void executeErrorResponseRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(PostgresqlServerErrorException.class);
    }

    @Test
    void executeRowDescriptionRows() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query"))
            .thenRespond(
                new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500))),
                new DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))),
                new CommandComplete("test", null, null))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .expectNext(new PostgresqlRow(MockCodecs.empty(), Collections.singletonList(new PostgresqlRow.Column(TEST.buffer(4).writeInt(100), 200, FORMAT_TEXT, "test-name"))))
            .verifyComplete();
    }

    @Test
    void executeRowDescriptionRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query"))
            .thenRespond(
                new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_TEXT, "test-name", 500))),
                new DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))),
                new CommandComplete("test", null, null))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "test-query")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValues() {
        Client client = TestClient.builder()
            .expectRequest(new Query("INSERT test-query RETURNING *")).thenRespond(new CommandComplete("test", null, 1))
            .build();

        new SimpleQueryPostgresqlStatement(client, MockCodecs.empty(), "INSERT test-query")
            .returnGeneratedValues()
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValuesHasReturningClause() {
        assertThatIllegalStateException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "RETURNING").returnGeneratedValues())
            .withMessage("Statement already includes RETURNING clause");
    }

    @Test
    void returnGeneratedValuesUnsupportedCommand() {
        assertThatIllegalStateException().isThrownBy(() -> new SimpleQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), "SELECT").returnGeneratedValues())
            .withMessage("Statement is not a DELETE, INSERT, or UPDATE command");
    }

    @Test
    void supportsMultilineParameterSymbol() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-0\ntest-query-$1")).isFalse();
    }

    @Test
    void supportsNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> SimpleQueryPostgresqlStatement.supports(null))
            .withMessage("sql must not be null");
    }

    @Test
    void supportsParameterSymbol() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-$1")).isFalse();
    }

    @Test
    void supportsQueryEmpty() {
        assertThat(SimpleQueryPostgresqlStatement.supports(" ")).isTrue();
    }

    @Test
    void supportsSemicolon() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query-1 ; test-query-2")).isTrue();
    }

    @Test
    void supportsSimple() {
        assertThat(SimpleQueryPostgresqlStatement.supports("test-query")).isTrue();
    }

}
