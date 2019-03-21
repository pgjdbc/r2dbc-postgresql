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
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class IndefiniteStatementCacheTest {

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IndefiniteStatementCache(null))
            .withMessage("client must not be null");
    }

    @Test
    void getName() {
        // @formatter:off
        Client client = TestClient.builder()
            .expectRequest(new Parse("S_0", Collections.singletonList(100), "test-query"), new Describe("S_0", ExecutionType.STATEMENT), Sync.INSTANCE)
                .thenRespond(ParseComplete.INSTANCE)
            .expectRequest(new Parse("S_1", Collections.singletonList(200), "test-query"), new Describe("S_1", ExecutionType.STATEMENT), Sync.INSTANCE)
                .thenRespond(ParseComplete.INSTANCE)
            .expectRequest(new Parse("S_2", Collections.singletonList(200), "test-query-2"), new Describe("S_2", ExecutionType.STATEMENT), Sync.INSTANCE)
                .thenRespond(ParseComplete.INSTANCE)
            .build();
        // @formatter:on

        IndefiniteStatementCache statementCache = new IndefiniteStatementCache(client);

        statementCache.getName(new Binding().add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(100))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();

        statementCache.getName(new Binding().add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(200))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();

        statementCache.getName(new Binding().add(0, new Parameter(FORMAT_BINARY, 200, TEST.buffer(2).writeShort(300))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_1")
            .verifyComplete();

        statementCache.getName(new Binding().add(0, new Parameter(FORMAT_BINARY, 200, TEST.buffer(4).writeShort(300))), "test-query-2")
            .as(StepVerifier::create)
            .expectNext("S_2")
            .verifyComplete();
    }

    @Test
    void getNameErrorResponse() {
        // @formatter:off
        Client client = TestClient.builder()
            .expectRequest(new Parse("S_0", Collections.singletonList(100), "test-query"), new Describe("S_0", ExecutionType.STATEMENT), Sync.INSTANCE)
                .thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();
        // @formatter:on

        IndefiniteStatementCache statementCache = new IndefiniteStatementCache(client);

        statementCache.getName(new Binding().add(0, new Parameter(FORMAT_BINARY, 100, TEST.buffer(4).writeInt(200))), "test-query")
            .as(StepVerifier::create)
            .verifyError(PostgresqlServerErrorException.class);
    }

    @Test
    void getNameNoBinding() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IndefiniteStatementCache(NO_OP).getName(null, "test-query"))
            .withMessage("binding must not be null");
    }

    @Test
    void getNameNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new IndefiniteStatementCache(NO_OP).getName(new Binding(), null))
            .withMessage("sql must not be null");
    }

}
