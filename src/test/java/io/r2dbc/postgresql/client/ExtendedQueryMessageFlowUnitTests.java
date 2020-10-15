/*
 * Copyright 2017-2020 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.backend.BindComplete;
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.NoData;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.backend.PortalSuspended;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.CompositeFrontendMessage;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Flush;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ExtendedQueryMessage}.
 */
final class ExtendedQueryMessageFlowUnitTests {

    @Test
    void execute() {
        Binding binding = new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(200))));

        Client client = TestClient.builder()
            .expectRequest(
                new CompositeFrontendMessage(new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(200)), emptyList(), "test-name"),
                    new Describe("B_0", ExecutionType.PORTAL)),
                new CompositeFrontendMessage(new Execute("B_0", 0),
                    new Close("B_0", ExecutionType.PORTAL),
                    Sync.INSTANCE)
            )
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE
            )
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        ExtendedQueryMessageFlow.execute(binding, client, portalNameSupplier, "test-name", "", false, Execute.NO_LIMIT)
            .as(StepVerifier::create)
            .expectNext(BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    @Disabled("TestClient doesn't work that way")
    void executeWithFetchSize() {
        Binding binding = new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(200))));

        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(200)), emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 1),
                Flush.INSTANCE
            )
            .thenRespond(BindComplete.INSTANCE, new RowDescription(emptyList()), new DataRow(), PortalSuspended.INSTANCE)
            .expectRequest(new Execute("B_0", 1), Flush.INSTANCE)
            .thenRespond(BindComplete.INSTANCE, new RowDescription(emptyList()), new DataRow(), PortalSuspended.INSTANCE)
            .expectRequest(new Execute("B_0", 1), Flush.INSTANCE)
            .thenRespond(new CommandComplete("test", null, null))
            .expectRequest(new Close("B_0", ExecutionType.PORTAL), Sync.INSTANCE)
            .thenRespond(CloseComplete.INSTANCE)
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        ExtendedQueryMessageFlow.execute(binding, client, portalNameSupplier, "test-name", "", false, 1)
            .as(StepVerifier::create)
            .expectNext(BindComplete.INSTANCE, new RowDescription(emptyList()), new DataRow(), new DataRow(), new CommandComplete("test", null, null), CloseComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    void executeNoBindings() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(null, NO_OP, () -> "", "test-statement", "", false, Execute.NO_LIMIT))
            .withMessage("binding must not be null");
    }

    @Test
    void executeNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(new Binding(0), null, () -> "", "test-statement", "", false, Execute.NO_LIMIT))
            .withMessage("client must not be null");
    }

    @Test
    void executeNoPortalNameSupplier() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(new Binding(0), NO_OP, null, "test-statement", "", false, Execute.NO_LIMIT))
            .withMessage("portalNameSupplier must not be null");
    }

    @Test
    void executeNoStatement() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(new Binding(0), NO_OP, () -> "", null, "", false, Execute.NO_LIMIT))
            .withMessage("statementName must not be null");
    }

    @Test
    void parse() {
        Client client = TestClient.builder()
            .expectRequest(new CompositeFrontendMessage(new Parse("test-name", new int[]{100}, "test-query"), Flush.INSTANCE))
            .thenRespond(ParseComplete.INSTANCE)
            .build();

        ExtendedQueryMessageFlow
            .parse(client, "test-name", "test-query", new int[]{100})
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void parseWithError() {
        Client client = TestClient.builder()
            .expectRequest(new CompositeFrontendMessage(new Parse("test-name", new int[]{100}, "test-query"), Flush.INSTANCE))
            .thenRespond(new ErrorResponse(emptyList()))
            .expectRequest(Sync.INSTANCE)
            .thenRespond(new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE))
            .build();

        ExtendedQueryMessageFlow
            .parse(client, "test-name", "test-query", new int[]{100})
            .as(StepVerifier::create)
            .expectNext(new ErrorResponse(emptyList()))
            .verifyComplete();
    }

    @Test
    void parseNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.parse(null, "test-name", "test-query", new int[0]))
            .withMessage("client must not be null");
    }

    @Test
    void parseNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.parse(NO_OP, null, "test-query", new int[0]))
            .withMessage("name must not be null");
    }

    @Test
    void parseNoQuery() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.parse(NO_OP, "test-name", null, new int[0]))
            .withMessage("query must not be null");
    }

    @Test
    void parseNoTypes() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.parse(NO_OP, "test-name", "test-query", null))
            .withMessage("types must not be null");
    }

    @Test
    void closeStatement() {
        Client client = TestClient.builder()
            .expectRequest(new CompositeFrontendMessage(new Close("test-name", ExecutionType.STATEMENT), Sync.INSTANCE))
            .thenRespond(CloseComplete.INSTANCE)
            .build();

        ExtendedQueryMessageFlow
            .closeStatement(client, "test-name")
            .as(StepVerifier::create)
            .expectNext(CloseComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    void closeStatementNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.closeStatement(null, "test-name"))
            .withMessage("client must not be null");
    }

    @Test
    void closeStatementNoName() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.closeStatement(NO_OP, null))
            .withMessage("name must not be null");
    }

}
