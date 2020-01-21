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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.EmptyQueryResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlResultTest {

    @Test
    void constructorNoRowMetadata() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlResult(MockContext.empty(), null, ExceptionFactory.INSTANCE))
            .withMessage("messages must not be null");
    }

    @Test
    void toResultCommandComplete() {
        PostgresqlResult result = PostgresqlResult.toResult(MockContext.empty(), Flux.just(new CommandComplete("test", null, 1)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void toResultEmptyQueryResponse() {
        PostgresqlResult result = PostgresqlResult.toResult(MockContext.empty(), Flux.just(EmptyQueryResponse.INSTANCE), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void toResultNoContext() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlResult.toResult(null, Flux.empty(), ExceptionFactory.INSTANCE))
            .withMessage("context must not be null");
    }

    @Test
    void toResultNoMessages() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlResult.toResult(MockContext.empty(), null, ExceptionFactory.INSTANCE))
            .withMessage("messages must not be null");
    }

    @Test
    void toResultRowDescriptionRowsUpdated() {
        PostgresqlResult result = PostgresqlResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), new DataRow(), new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void toResultRowDescriptionMap() {
        PostgresqlResult result = PostgresqlResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), new DataRow(), new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

}
