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

import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PostgresqlCopyOutResult}.
 */
final class PostgresqlCopyOutResultUnitTests {

    @Test
    void toCopyOutResultCommandComplete() {
        PostgresqlCopyOutResult result = PostgresqlCopyOutResult.toCopyOutResult(Flux.just(new CommandComplete("test", null, 0L)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();
    }
    
    @Test
    void toCopyOutResultNoMessages() {
        assertThatIllegalArgumentException().isThrownBy(() -> PostgresqlCopyOutResult.toCopyOutResult(null, ExceptionFactory.INSTANCE))
            .withMessage("messages must not be null");
    }

    @Test
    void toCopyOutResultResponseMetadataMap() {
        PostgresqlCopyOutResult result = PostgresqlCopyOutResult.toCopyOutResult(Flux.just(new CopyOutResponse(Collections.emptyList(), Format.FORMAT_BINARY), new CopyData(Unpooled.EMPTY_BUFFER), CopyDone.INSTANCE, new CommandComplete("test", null, null)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void toCopyOutResultContainsRowDescription() {
        io.r2dbc.postgresql.api.PostgresqlCopyOutResult result = PostgresqlCopyOutResult.toCopyOutResult(Flux.just(new RowDescription(Collections.emptyList()), new CommandComplete("test", null, null)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyErrorMatches(e -> e.getMessage().contains("copyOut may only be used"));
    }

}
