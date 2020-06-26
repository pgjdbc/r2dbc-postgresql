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

import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.frontend.Query;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link SimpleQueryMessageFlow}.
 */
final class SimpleQueryMessageFlowUnitTests {

    @Test
    void exchange() {
        Client client = TestClient.builder()
            .expectRequest(new Query("test-query")).thenRespond(new CommandComplete("test", null, null))
            .build();

        SimpleQueryMessageFlow
            .exchange(client, "test-query")
            .as(StepVerifier::create)
            .expectNext(new CommandComplete("test", null, null))
            .verifyComplete();
    }

    @Test
    void exchangeNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> SimpleQueryMessageFlow.exchange(null, "test-query"))
            .withMessage("client must not be null");
    }

    @Test
    void exchangeNoQuery() {
        assertThatIllegalArgumentException().isThrownBy(() -> SimpleQueryMessageFlow.exchange(mock(Client.class), null))
            .withMessage("query must not be null");
    }

}
