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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.backend.BindComplete;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.NoData;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ExtendedQueryMessageFlowTest {

    @Test
    void execute() {
        List<Binding> bindings = Arrays.asList(
            new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(200)))),
            new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(300))))
        );

        Client client = TestClient.builder()
            .expectRequest(
                new Bind("B_0", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(200)), Collections.emptyList(), "test-name"),
                new Describe("B_0", ExecutionType.PORTAL),
                new Execute("B_0", 0),
                new Close("B_0", ExecutionType.PORTAL),
                new Bind("B_1", Collections.singletonList(FORMAT_BINARY), Collections.singletonList(TEST.buffer(4).writeInt(300)), Collections.emptyList(), "test-name"),
                new Describe("B_1", ExecutionType.PORTAL),
                new Execute("B_1", 0),
                new Close("B_1", ExecutionType.PORTAL),
                Sync.INSTANCE)
            .thenRespond(
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null),
                BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null)
            )
            .build();

        PortalNameSupplier portalNameSupplier = new LinkedList<>(Arrays.asList("B_0", "B_1"))::remove;

        ExtendedQueryMessageFlow
            .execute(bindings, client, portalNameSupplier, "test-name", "", false)
            .as(StepVerifier::create)
            .expectNext(BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null))
            .expectNext(BindComplete.INSTANCE, NoData.INSTANCE, new CommandComplete("test", null, null))
            .verifyComplete();
    }

    @Test
    void executeNoBindings() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(null, NO_OP, () -> "", "test-statement", "", false))
            .withMessage("bindings must not be null");
    }

    @Test
    void executeNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(Collections.emptyList(), null, () -> "", "test-statement", "", false))
            .withMessage("client must not be null");
    }

    @Test
    void executeNoPortalNameSupplier() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(Collections.emptyList(), NO_OP, null, "test-statement", "", false))
            .withMessage("portalNameSupplier must not be null");
    }

    @Test
    void executeNoStatement() {
        assertThatIllegalArgumentException().isThrownBy(() -> ExtendedQueryMessageFlow.execute(Collections.emptyList(), NO_OP, () -> "", null, "", false))
            .withMessage("statementName must not be null");
    }

    @Test
    void parse() {
        Client client = TestClient.builder()
            .expectRequest(new Parse("test-name", new int[]{100}, "test-query"), new Describe("test-name", ExecutionType.STATEMENT), Sync.INSTANCE)
            .thenRespond(ParseComplete.INSTANCE)
            .build();

        ExtendedQueryMessageFlow
            .parse(client, "test-name", "test-query", new int[]{100})
            .as(StepVerifier::create)
            .expectNext(ParseComplete.INSTANCE)
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

}
