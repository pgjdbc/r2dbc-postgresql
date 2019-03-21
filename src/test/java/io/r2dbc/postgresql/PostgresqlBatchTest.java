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

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.codec.MockCodecs;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.frontend.Query;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Collections;

import static io.r2dbc.postgresql.client.TestClient.NO_OP;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlBatchTest {

    @Test
    void add() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query-1; test-query-2")).thenRespond(new CommandComplete("test-1", null, null), new CommandComplete("test-2", null, null))
            .build();

        new PostgresqlBatch(client, MockCodecs.empty())
            .add("test-query-1")
            .add("test-query-2")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void addNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlBatch(NO_OP, MockCodecs.empty()).add(null))
            .withMessage("sql must not be null");
    }

    @Test
    void addWithParameter() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlBatch(NO_OP, MockCodecs.empty()).add("test-query-$1"))
            .withMessage("Statement 'test-query-$1' is not supported.  This is often due to the presence of parameters.");
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlBatch(null, MockCodecs.empty()))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoCodecs() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlBatch(NO_OP, null))
            .withMessage("codecs must not be null");
    }

    @Test
    void executeCommandComplete() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, null))
            .build();

        new PostgresqlBatch(client, MockCodecs.empty())
            .add("test-query")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void executeEmptyQueryResponse() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(EmptyQueryResponse.INSTANCE)
            .build();

        new PostgresqlBatch(client, MockCodecs.empty())
            .add("test-query")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void executeErrorResponse() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new ErrorResponse(Collections.emptyList()))
            .build();

        new PostgresqlBatch(client, MockCodecs.empty())
            .add("test-query")
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

}
