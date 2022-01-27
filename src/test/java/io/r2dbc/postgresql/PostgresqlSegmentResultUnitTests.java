/*
 * Copyright 2021 the original author or authors.
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

import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.Field.FieldType;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostgresqlSegmentResult}.
 */
class PostgresqlSegmentResultUnitTests {

    @Test
    void shouldApplyRowMapping() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), new DataRow(), new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void mapShouldIgnoreNotice() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new NoticeResponse(Collections.emptyList())), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void mapShouldTerminateWithError() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new ErrorResponse(Collections.emptyList())), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientException.class);
    }

    @Test
    void getRowsUpdatedShouldTerminateWithError() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new ErrorResponse(Collections.emptyList())), ExceptionFactory.INSTANCE);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientException.class);
    }

    @Test
    void shouldConsumeRowsUpdated() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new CommandComplete
            ("test", null, 42)), ExceptionFactory.INSTANCE);

        result.getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(42L)
            .verifyComplete();
    }

    @Test
    void filterShouldRetainUpdateCount() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new CommandComplete
            ("test", null, 42)), ExceptionFactory.INSTANCE);

        result.filter(Result.UpdateCount.class::isInstance).getRowsUpdated()
            .as(StepVerifier::create)
            .expectNext(42L)
            .verifyComplete();
    }

    @Test
    void filterShouldSkipRowMapping() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), new DataRow(), new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result = result.filter(it -> false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void filterShouldSkipErrorMessage() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new ErrorResponse(Collections.emptyList()), new RowDescription(Collections.emptyList()),
            new DataRow(), new CommandComplete
                ("test", null, null)), ExceptionFactory.INSTANCE);

        result = result.filter(Result.RowSegment.class::isInstance);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void mapShouldDeallocateResources() {

        DataRow dataRow = new DataRow();
        assertThat(dataRow.refCnt()).isOne();
        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), dataRow, new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void filterShouldDeallocateResources() {

        DataRow dataRow = new DataRow();
        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), dataRow, new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        result = result.filter(it -> false);

        result.map((row, rowMetadata) -> row)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void flatMapShouldDeallocateResourcesAfterConsumption() {

        DataRow dataRow = new DataRow();
        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), dataRow, new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        Flux.from(result.flatMap(Mono::just))
            .map(it -> {
                assertThat(((ReferenceCounted) it).refCnt()).isOne();
                return it;
            })
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void flatMapShouldNotTerminateWithError() {

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new ErrorResponse(Collections.emptyList()), new RowDescription(Collections.emptyList()),
            new DataRow(), new CommandComplete
                ("test", null, 42)), ExceptionFactory.INSTANCE);

        Flux.from(result.flatMap(Mono::just))
            .as(StepVerifier::create)
            .expectNextCount(3)
            .verifyComplete();
    }

    @Test
    void emptyFlatMapShouldDeallocateResourcesAfterConsumption() {

        DataRow dataRow = new DataRow();
        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new RowDescription(Collections.emptyList()), dataRow, new CommandComplete
            ("test", null, null)), ExceptionFactory.INSTANCE);

        Flux.from(result.flatMap(data -> Mono.empty()))
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(dataRow.refCnt()).isZero();
    }

    @Test
    void flatMapShouldMapErrorResponse() {

        List<Field> fields = Arrays.asList(
            new Field(FieldType.CODE, "28000"),
            new Field(FieldType.MESSAGE, "error message desc")
        );

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new ErrorResponse(fields)), ExceptionFactory.INSTANCE);

        Flux.from(result.flatMap(data -> {

            assertThat(data).isInstanceOf(Result.Message.class);

            Result.Message message = (Result.Message) data;
            assertThat(message.errorCode()).isZero();
            assertThat(message.sqlState()).isEqualTo("28000");
            assertThat(message.message()).isEqualTo("error message desc");
            assertThat(message.exception()).isInstanceOf(R2dbcPermissionDeniedException.class);

            return Mono.just(data);
        }))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void flatMapShouldMapNoticeResponse() {

        List<Field> fields = Arrays.asList(
            new Field(FieldType.CODE, "28000"),
            new Field(FieldType.MESSAGE, "error message desc")
        );

        PostgresqlSegmentResult result = PostgresqlSegmentResult.toResult(MockContext.empty(), Flux.just(new NoticeResponse(fields)), ExceptionFactory.INSTANCE);

        Flux.from(result.flatMap(data -> {

            assertThat(data).isInstanceOf(Result.Message.class);

            Result.Message message = (Result.Message) data;
            assertThat(message.errorCode()).isZero();
            assertThat(message.sqlState()).isEqualTo("28000");
            assertThat(message.message()).isEqualTo("error message desc");
            assertThat(message.exception()).isInstanceOf(R2dbcPermissionDeniedException.class);

            return Mono.just(data);
        }))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

}
