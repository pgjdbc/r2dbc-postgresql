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

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.client.Version;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.postgresql.message.frontend.Terminate;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.client.TransactionStatus.FAILED;
import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.client.TransactionStatus.OPEN;
import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link PostgresqlConnection}.
 */
final class PostgresqlConnectionUnitTests {

    private final StatementCache statementCache = mock(StatementCache.class, RETURNS_SMART_NULLS);

    @Test
    void beginTransaction() {
        Client client = TestClient.builder()
            .expectRequest(new Query("BEGIN")).thenRespond(new CommandComplete("BEGIN", null, null))
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);
        assertThat(connection.isAutoCommit()).isTrue();

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void beginTransactionErrorResponse() {
        Client client = TestClient.builder()
            .expectRequest(new Query("BEGIN")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .beginTransaction()
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void beginTransactionNonIdle() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void close() {
        Client client = TestClient.builder()
            .expectRequest(Terminate.INSTANCE).thenRespond()
            .expectClose()
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .close()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void commitTransaction() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("COMMIT")).thenRespond(new CommandComplete("COMMIT", null, null))
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        assertThat(connection.isAutoCommit()).isFalse();
        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void commitTransactionErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("COMMIT")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .commitTransaction()
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void commitTransactionNonOpen() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(null, MockCodecs.empty(), this.statementCache))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoCodec() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, null, this.statementCache))
            .withMessage("codecs must not be null");
    }

    @Test
    void constructorNoPortalNameSupplier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlConnection(NO_OP, MockCodecs.empty(), null, this.statementCache, IsolationLevel.READ_COMMITTED, null))
            .withMessage("portalNameSupplier must not be null");
    }

    @Test
    void constructorNoStatementCache() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), null))
            .withMessage("statementCache must not be null");
    }

    @Test
    void createBatch() {
        assertThat(createConnection(NO_OP, MockCodecs.empty(), this.statementCache).createBatch()).isInstanceOf(PostgresqlBatch.class);
    }

    @Test
    void createSavepoint() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("SAVEPOINT test-name")).thenRespond(new CommandComplete("SAVEPOINT", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .createSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void createSavepointErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("SAVEPOINT test-name")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .createSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void createSavepointNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), this.statementCache).createSavepoint(null))
            .withMessage("name must not be null");
    }

    @Test
    void createSavepointNonOpen() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .createSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void createStatementExtended() {
        assertThat(createConnection(NO_OP, MockCodecs.empty(), this.statementCache).createStatement("test-query-$1")).isInstanceOf(ExtendedQueryPostgresqlStatement.class);
    }

    @Test
    void createStatementIllegal() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), this.statementCache).createStatement("test-query-$1-1 ; " +
            "test-query-$1-2"))
            .withMessage("Statement 'test-query-$1-1 ; test-query-$1-2' cannot be created. This is often due to the presence of both multiple statements and parameters at the same time.");
    }

    @Test
    void createStatementSimple() {
        assertThat(createConnection(NO_OP, MockCodecs.empty(), this.statementCache).createStatement("test-query-1; test-query-2")).isInstanceOf(SimpleQueryPostgresqlStatement.class);
    }

    @Test
    void releaseSavepoint() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("RELEASE SAVEPOINT test-name")).thenRespond(new CommandComplete("RELEASE", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .releaseSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void releaseSavepointErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("RELEASE SAVEPOINT test-name")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .releaseSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void releaseSavepointNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), this.statementCache).releaseSavepoint(null))
            .withMessage("name must not be null");
    }

    @Test
    void releaseSavepointNonOpen() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .releaseSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void rollbackTransaction() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("ROLLBACK")).thenRespond(new CommandComplete("ROLLBACK", null, null))
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void rollbackTransactionErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("ROLLBACK")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransaction()
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void rollbackTransactionIdle() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void rollbackTransactionFailed() {
        Client client = TestClient.builder()
            .transactionStatus(FAILED)
            .expectRequest(new Query("ROLLBACK")).thenRespond(new CommandComplete("ROLLBACK", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void rollbackTransactionToSavepoint() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("ROLLBACK TO SAVEPOINT test-name")).thenRespond(new CommandComplete("ROLLBACK", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransactionToSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void rollbackTransactionToSavepointErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("ROLLBACK TO SAVEPOINT test-name")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransactionToSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void rollbackTransactionToSavepointNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), this.statementCache).rollbackTransactionToSavepoint(null))
            .withMessage("name must not be null");
    }

    @Test
    void rollbackTransactionToSavepointNonOpen() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .rollbackTransactionToSavepoint("test-name")
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void getMetadata() {
        Client client = TestClient.builder().withVersion(new Version("9.4")).build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        PostgresqlConnectionMetadata metadata = connection.getMetadata();

        assertThat(metadata.getDatabaseProductName()).isEqualTo("PostgreSQL");
        assertThat(metadata.getDatabaseVersion()).isEqualTo("9.4");
    }

    @Test
    void isAutoCommitFalseOnOpenTransaction() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        assertThat(connection.isAutoCommit()).isFalse();
    }

    @Test
    void isAutoCommitTrueByDefault() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        assertThat(connection.isAutoCommit()).isTrue();
    }

    @Test
    void setAutoCommitFalseBeginsTransaction() {
        Client client = TestClient.builder()
            .expectRequest(new Query("BEGIN")).thenRespond(new CommandComplete("BEGIN", null, null))
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        connection.setAutoCommit(false)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void setAutoCommitTrueIsNoOpBeginsTransaction() {
        Client client = TestClient.builder()
            .build();

        PostgresqlConnection connection = createConnection(client, MockCodecs.empty(), this.statementCache);

        connection.setAutoCommit(true)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(connection.isAutoCommit()).isTrue();
    }

    @Test
    void setTransactionIsolationLevel() {
        Client client = TestClient.builder()
            .transactionStatus(OPEN)
            .expectRequest(new Query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")).thenRespond(new CommandComplete("SET", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .setTransactionIsolationLevel(READ_COMMITTED)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void setTransactionIsolationLevelErrorResponse() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .expectRequest(new Query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .setTransactionIsolationLevel(READ_COMMITTED)
            .as(StepVerifier::create)
            .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void setTransactionIsolationLevelNoIsolationLevel() {
        assertThatIllegalArgumentException().isThrownBy(() -> createConnection(NO_OP, MockCodecs.empty(), this.statementCache).setTransactionIsolationLevel(null))
            .withMessage("isolationLevel must not be null");
    }

    @Test
    void setTransactionIsolationLevelNonOpen() {
        Client client = TestClient.builder()
            .transactionStatus(IDLE)
            .expectRequest(new Query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")).thenRespond(new CommandComplete("SET", null, null))
            .build();

        createConnection(client, MockCodecs.empty(), this.statementCache)
            .setTransactionIsolationLevel(READ_COMMITTED)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    private PostgresqlConnection createConnection(Client client, MockCodecs codecs, StatementCache cache) {
        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .host("127.0.0.1")
            .username("admin")
            .password("password")
            .build();
        return new PostgresqlConnection(client, codecs, () -> "", cache, IsolationLevel.READ_COMMITTED, configuration);
    }

}
