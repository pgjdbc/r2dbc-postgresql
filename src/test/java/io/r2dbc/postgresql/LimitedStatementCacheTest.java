/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Flush;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class LimitedStatementCacheTest {

    @Test
    void constructorInvalidLimit() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedStatementCache(NO_OP, -1))
            .withMessage("statement cache limit must be greater than zero");
        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedStatementCache(NO_OP, 0))
            .withMessage("statement cache limit must be greater than zero");
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedStatementCache(null, 2))
            .withMessage("client must not be null");
    }

    @Test
    void getName() {
        // @formatter:off
        Client client = TestClient.builder()
            .expectRequest(new Parse("S_0", new int[]{100}, "test-query"),  Flush.INSTANCE)
            .thenRespond(ParseComplete.INSTANCE)
            .expectRequest(new Parse("S_1", new int[]{200}, "test-query"), Flush.INSTANCE)
            .thenRespond(ParseComplete.INSTANCE)
            .expectRequest(new Close("S_0", ExecutionType.STATEMENT), Sync.INSTANCE)
            .thenRespond(CloseComplete.INSTANCE)
            .expectRequest(new Parse("S_2", new int[]{200}, "test-query-2"), Flush.INSTANCE)
            .thenRespond(ParseComplete.INSTANCE)
            .expectRequest(new Close("S_2", ExecutionType.STATEMENT), Sync.INSTANCE)
            .thenRespond(CloseComplete.INSTANCE)
            .expectRequest(new Parse("S_0", new int[]{100}, "test-query"),  Flush.INSTANCE)
            .thenRespond(ParseComplete.INSTANCE)
            .build();
        // @formatter:on

        LimitedStatementCache statementCache = new LimitedStatementCache(client, 2);

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(100)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(200)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 200, Flux.just(TEST.buffer(2).writeShort(300)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_1")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 200, Flux.just(TEST.buffer(4).writeShort(300)))), "test-query-2")
            .as(StepVerifier::create)
            .expectNext("S_2")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 200, Flux.just(TEST.buffer(2).writeShort(300)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_1")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(100)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(100)))), "test-query")
            .as(StepVerifier::create)
            .expectNext("S_0")
            .verifyComplete();
    }

    @Test
    void getNameErrorResponse() {
        // @formatter:off
        Client client = TestClient.builder()
            .expectRequest(new Parse("S_0", new int[]{100}, "test-query"),  Flush.INSTANCE)
            .thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();
        // @formatter:on

        LimitedStatementCache statementCache = new LimitedStatementCache(client, 2);

        statementCache.getName(new Binding(1).add(0, new Parameter(FORMAT_BINARY, 100, Flux.just(TEST.buffer(4).writeInt(200)))), "test-query")
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void getNameNoBinding() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedStatementCache(NO_OP, 2).getName(null, "test-query"))
            .withMessage("binding must not be null");
    }

    @Test
    void getNameNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LimitedStatementCache(NO_OP, 2).getName(new Binding(0), null))
            .withMessage("sql must not be null");
    }

}
