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

import io.r2dbc.postgresql.message.frontend.CancelRequest;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

final class CancelRequestMessageFlowTest {

    @Test
    void exchange() {
        Client client = TestClient.builder()
            .processId(100)
            .secretKey(200)
            .expectRequest(new CancelRequest(100, 200)).thenRespond()
            .build();

        CancelRequestMessageFlow
            .exchange(client)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void exchangeNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> CancelRequestMessageFlow.exchange(null))
            .withMessage("client must not be null");
    }

    @Test
    void exchangeNoProcessId() {
        Client client = TestClient.builder()
            .build();

        assertThatIllegalStateException().isThrownBy(() -> CancelRequestMessageFlow.exchange(client))
            .withMessage("Connection does not yet have a processId");
    }

    @Test
    void exchangeNoSecretKey() {
        Client client = TestClient.builder()
            .processId(100)
            .build();

        assertThatIllegalStateException().isThrownBy(() -> CancelRequestMessageFlow.exchange(client))
            .withMessage("Connection does not yet have a secretKey");
    }

}
