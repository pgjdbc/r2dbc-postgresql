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

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.BindComplete;
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.NoData;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Sync;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ExtendedQueryPostgresqlStatementTest {

    private final Parameter parameter = new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100)));

    private final MockCodecs codecs = MockCodecs.builder().encoding(100, this.parameter).build();

    private final StatementCache statementCache = mock(StatementCache.class, RETURNS_SMART_NULLS);

    private final ExtendedQueryPostgresqlStatement statement = new ExtendedQueryPostgresqlStatement(NO_OP, this.codecs, () -> "", "test-query-$1", this.statementCache, false);

    @Test
    void bind() {
        assertThat(this.statement.bind("$1", 100).getCurrentBinding()).isEqualTo(new Binding(1).add(0, this.parameter));
    }

    @Test
    void bindIndex() {
        assertThat(((ExtendedQueryPostgresqlStatement) this.statement.bind(0, 100)).getCurrentBinding()).isEqualTo(new Binding(1).add(0, this.parameter));
    }

    @Test
    void bindIndexNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(1, null))
            .withMessage("value must not be null");
    }

    @Test
    void bindNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(null, ""))
            .withMessage("identifier must not be null");
    }

    @Test
    void bindNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("$1", null))
            .withMessage("value must not be null");
    }

    @Test
    void bindNull() {
        MockCodecs codecs = MockCodecs.builder()
            .encoding(Integer.class, new Parameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE))
            .build();

        ExtendedQueryPostgresqlStatement statement = new ExtendedQueryPostgresqlStatement(NO_OP, codecs, () -> "", "test-query-$1", this.statementCache, false);

        assertThat(statement.bindNull("$1", Integer.class).getCurrentBinding())
            .isEqualTo(new Binding(1).add(0, new Parameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE)));
    }

    @Test
    void bindNullIndexNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull(0, null))
            .withMessage("type must not be null");
    }

    @Test
    void bindNullNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull(null, Integer.class))
            .withMessage("identifier must not be null");
    }

    @Test
    void bindNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull("$1", null))
            .withMessage("type must not be null");
    }

    @Test
    void bindNullWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull("foo", Integer.class))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '\\$([\\d]+)'.");
    }

    @Test
    void bindNullWrongIdentifierType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull(new Object(), Integer.class))
            .withMessage("identifier must be a String");
    }

    @Test
    void bindWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("foo", ""))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '\\$([\\d]+)'.");
    }

    @Test
    void bindWrongIdentifierType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(new Object(), ""))
            .withMessage("identifier must be a String");
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(null, MockCodecs.empty(), () -> "", "test-query", this.statementCache, false))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, null, () -> "", "test-query", this.statementCache, false))
            .withMessage("codecs must not be null");
    }

    @Test
    void constructorNoPortalNameSupplier() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), null, "test-query", this.statementCache, false))
            .withMessage("portalNameSupplier must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), () -> "", null, this.statementCache, false))
            .withMessage("sql must not be null");
    }

    @Test
    void constructorNoStatementCache() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), () -> "", "test-query", null, false))
            .withMessage("statementCache must not be null");
    }

    @Test
    void execute() {
        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                new Bind("B_1", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(200)), Collections.emptyList(), "test-name"),
                new Describe("B_1", ExecutionType.PORTAL),
                new Execute("B_1", 0),
                new Close("B_1", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE,
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE
            )
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .encoding(200, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(200))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        when(this.statementCache.getName(any(), any())).thenReturn(Mono.just("test-name"));

        new ExtendedQueryPostgresqlStatement(client, codecs, portalNameSupplier, "test-query-$1-$1", this.statementCache, false)
            .bind("$1", 100)
            .add()
            .bind("$1", 200)
            .add()
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void executeEmpty() {
        assertThatIllegalStateException().isThrownBy(this.statement::execute)
            .withMessage("No parameters have been bound");
    }

    @Test
    void executeErrorResponse() {
        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        when(this.statementCache.getName(any(), any())).thenReturn(Mono.just("test-name"));

        new ExtendedQueryPostgresqlStatement(client, codecs, portalNameSupplier, "test-query-$1", this.statementCache, false)
            .bind("$1", 100)
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyError(PostgresqlServerErrorException.class);
    }

    @Test
    void executeWithoutAdd() {
        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        when(this.statementCache.getName(any(), any())).thenReturn(Mono.just("test-name"));

        new ExtendedQueryPostgresqlStatement(client, codecs, portalNameSupplier, "test-query-$1", this.statementCache, false)
            .bind("$1", 100)
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void executeWithoutResultWithMap() {
        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        when(this.statementCache.getName(any(), any())).thenReturn(Mono.just("test-name"));

        new ExtendedQueryPostgresqlStatement(client, codecs, portalNameSupplier, "test-query-$1", this.statementCache, false)
            .bind("$1", 100)
            .execute()
            .flatMap(result -> result.map((row, metadata) -> 1))
            .timeout(Duration.ofSeconds(1))
            .as(StepVerifier::create)
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValues() {
        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new Parameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        when(this.statementCache.getName(any(), any())).thenReturn(Mono.just("test-name"));

        new ExtendedQueryPostgresqlStatement(client, codecs, portalNameSupplier, "INSERT test-query-$1", this.statementCache, false)
            .bind("$1", 100)
            .returnGeneratedValues()
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

    }

    @Test
    void returnGeneratedValuesHasReturningClause() {
        assertThatIllegalStateException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), () -> "", "RETURNING", this.statementCache, false).returnGeneratedValues())
            .withMessage("Statement already includes RETURNING clause");
    }

    @Test
    void returnGeneratedValuesUnsupportedCommand() {
        assertThatIllegalStateException().isThrownBy(() -> new ExtendedQueryPostgresqlStatement(NO_OP, MockCodecs.empty(), () -> "", "SELECT", this.statementCache, false).returnGeneratedValues())
            .withMessage("Statement is not a DELETE, INSERT, or UPDATE command");
    }

    @Test
    void supportsMultilineParameterSymbol() {
        assertThat(ExtendedQueryPostgresqlStatement.supports("test-query-0\ntest-query-$1")).isTrue();
    }

    @Test
    void supportsNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryPostgresqlStatement.supports(null))
            .withMessage("sql must not be null");
    }

    @Test
    void supportsParameterSymbol() {
        assertThat(ExtendedQueryPostgresqlStatement.supports("test-query-$1")).isTrue();
    }

    @Test
    void supportsQueryEmpty() {
        assertThat(ExtendedQueryPostgresqlStatement.supports(" ")).isFalse();
    }

    @Test
    void supportsSemicolon() {
        assertThat(ExtendedQueryPostgresqlStatement.supports("test-query-1; test-query-2")).isFalse();
    }

    @Test
    void supportsSimple() {
        assertThat(ExtendedQueryPostgresqlStatement.supports("test-query")).isFalse();
    }
}
