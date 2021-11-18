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

import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.client.*;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.*;
import io.r2dbc.postgresql.message.frontend.*;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PostgresqlStatement}.
 */
final class PostgresqlStatementUnitTests {

    private final EncodedParameter parameter = new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100)));

    private final MockCodecs codecs = MockCodecs.builder().encoding(100, this.parameter).build();

    private final PostgresqlStatement statement = new PostgresqlStatement(MockContext.builder().codecs(codecs).build(), "test-query-$1");

    @Test
    void bind() {
        assertThat(this.statement.bind("$1", 100).getCurrentBinding()).isEqualTo(new Binding(1).add(0, this.parameter));
    }

    @Test
    void bindIndex() {
        assertThat(this.statement.bind(0, 100).getCurrentBinding()).isEqualTo(new Binding(1).add(0, this.parameter));
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
            .encoding(Integer.class, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE))
            .build();

        PostgresqlStatement statement = new PostgresqlStatement(MockContext.builder().codecs(codecs).build(), "test-query-$1");

        assertThat(statement.bindNull("$1", Integer.class).getCurrentBinding())
            .isEqualTo(new Binding(1).add(0, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), NULL_VALUE)));
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
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> this.statement.bindNull("foo", Integer.class))
            .withMessage("\"foo\" is not a valid identifier");
    }

    @Test
    void bindWrongIdentifierFormat() {
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> this.statement.bind("foo", ""))
            .withMessage("\"foo\" is not a valid identifier");
    }

    @Test
    void constructorNoResources() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlStatement(null, "test-query"))
            .withMessage("resources must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlStatement(MockContext.empty(), null))
            .withMessage("sql must not be null");
    }

    @Test
    void executeEmpty() {
        assertThatIllegalStateException().isThrownBy(this.statement::execute)
            .withMessage("No parameters have been bound");
    }

    @Test
    void executeErrorAfterBind() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL),
                    new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(BindComplete.INSTANCE, new RowDescription(Collections.emptyList()), new ErrorResponse(Collections.emptyList()))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");

        new PostgresqlStatement(context, "test-query-$1")
            .bind("$1", 100)
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeErrorResponseRows() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL),
                    new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(BindComplete.INSTANCE, NoData.INSTANCE, new ErrorResponse(Collections.emptyList()))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");

        new PostgresqlStatement(context, "test-query-$1")
            .bind("$1", 100)
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeErrorResponseRowsUpdated() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL),
                    new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");

        new PostgresqlStatement(context, "test-query-$1")
            .bind("$1", 100)
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeErrorResponse() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL), new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");

        new PostgresqlStatement(context, "test-query-$1")
            .bind("$1", 100)
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void executeWithoutAdd() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL)),
                new CompositeFrontendMessage(new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");
        when(context.getStatementCache().requiresPrepare(any(), any())).thenReturn(true);

        new PostgresqlStatement(context, "test-query-$1")
            .bind("$1", 100)
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void releasesArgumentsOnCancel() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL), new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(
                new ErrorResponse(Collections.emptyList()))
            .build();

        AtomicBoolean hasReleased = new AtomicBoolean();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .encoding(200, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100)).doOnSubscribe(it -> hasReleased.set(true))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");
        when(context.getStatementCache().requiresPrepare(any(), any())).thenReturn(false);

        new PostgresqlStatement(context, "test-query-$1")
                .bind("$1", 100).add()
                .bind("$1", 200)
                .execute()
                .flatMap(PostgresqlResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(R2dbcNonTransientResourceException.class);

        assertThat(hasReleased).isTrue();
    }

    @Test
    void returnGeneratedValues() {
        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(100)), Collections.emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL)),
                new CompositeFrontendMessage(new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE))
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .build();

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, new EncodedParameter(FORMAT_BINARY, INT4.getObjectId(), Flux.just(TEST.buffer(4).writeInt(100))))
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;
        ConnectionResources context = MockContext.builder().client(client).codecs(codecs).portalNameSupplier(portalNameSupplier).build();

        when(context.getStatementCache().getName(any(), any())).thenReturn("test-name");
        when(context.getStatementCache().requiresPrepare(any(), any())).thenReturn(true);

        new PostgresqlStatement(context, "INSERT test-query-$1")
            .bind("$1", 100)
            .returnGeneratedValues()
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

    }

    @Test
    void returnGeneratedValuesHasReturningClause() {
        assertThatIllegalStateException().isThrownBy(() -> new PostgresqlStatement(MockContext.empty(), "RETURNING").returnGeneratedValues())
            .withMessage("Statement already includes RETURNING clause");
    }

    @Test
    void returnGeneratedValuesUnsupportedCommand() {
        assertThatIllegalStateException().isThrownBy(() -> new PostgresqlStatement(MockContext.empty(), "SELECT").returnGeneratedValues())
            .withMessage("Statement is not a DELETE, INSERT, or UPDATE command");
    }
}
