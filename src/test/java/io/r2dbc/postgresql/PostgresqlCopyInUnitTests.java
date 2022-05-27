/*
 * Copyright 2022 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.ExceptionFactory.PostgresqlNonTransientResourceException;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.CopyInResponse;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.CopyData;
import io.r2dbc.postgresql.message.frontend.CopyDone;
import io.r2dbc.postgresql.message.frontend.CopyFail;
import io.r2dbc.postgresql.message.frontend.Query;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static io.r2dbc.postgresql.message.backend.ReadyForQuery.TransactionStatus.IDLE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

/**
 * Unit tests for {@link PostgresqlCopyIn}.
 */
final class PostgresqlCopyInUnitTests {

    @Test
    void copyIn() {
        ByteBuf byteBuffer = byteBuf("a\n");
        Client client = TestClient.builder()
            .expectRequest(new Query("some-sql"), new CopyData(byteBuffer), CopyDone.INSTANCE)
            .thenRespond(
                new CopyInResponse(emptySet(), Format.FORMAT_TEXT),
                new CommandComplete("cmd", 1, 1),
                new ReadyForQuery(IDLE)
            ).build();

        new PostgresqlCopyIn(MockContext.builder().client(client).build())
            .copy("some-sql", Flux.just(Flux.just(byteBuffer)))
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();
    }

    @Test
    void copyInErrorResponse() {
        ByteBuf byteBuffer = byteBuf("a\n");
        Client client = TestClient.builder()
            .expectRequest(new Query("some-sql")).thenRespond(new ErrorResponse(emptyList()))
            .build();

        new PostgresqlCopyIn(MockContext.builder().client(client).build())
            .copy("some-sql", Flux.just(Flux.just(byteBuffer)))
            .as(StepVerifier::create)
            .expectError(PostgresqlNonTransientResourceException.class)
            .verify();
    }

    @Test
    void copyInEmpty() {
        Client client = TestClient.builder()
            .transactionStatus(TransactionStatus.IDLE)
            .expectRequest(new Query("some-sql"), CopyDone.INSTANCE).thenRespond(
                new CopyInResponse(emptySet(), Format.FORMAT_TEXT),
                new CommandComplete("cmd", 1, 0),
                new ReadyForQuery(ReadyForQuery.TransactionStatus.IDLE)
            )
            .build();

        new PostgresqlCopyIn(MockContext.builder().client(client).build())
            .copy("some-sql", Flux.empty())
            .as(StepVerifier::create)
            .expectNext(0L)
            .verifyComplete();
    }

    @Test
    void copyInError() {
        Publisher<ByteBuf> testPublisher = Flux.just(byteBuf("a\n"), byteBuf("b\n"));

        Client client = TestClient.builder()
            .expectRequest(new Query("some-sql"),
                new CopyData(byteBuf("a\nb\n")),
                new CopyFail("Copy operation failed: Failed")
            ).thenRespond(
                new CopyInResponse(emptySet(), Format.FORMAT_TEXT),
                new ErrorResponse(emptyList()),
                new ReadyForQuery(IDLE)
            ).build();

        new PostgresqlCopyIn(MockContext.builder().client(client).build())
            .copy("some-sql", Flux.concat(Mono.just(testPublisher), Mono.just(Mono.error(new RuntimeException("Failed")))))
            .as(StepVerifier::create)
            .verifyError(RuntimeException.class);
    }

    @Test
    void copyInCancel() {
        TestPublisher<ByteBuf> testPublisher = TestPublisher.create();

        Client client = TestClient.builder()
            .expectRequest(new Query("some-sql"),
                new CopyData(byteBuf("a")),
                new CopyData(byteBuf("b")),
                new CopyFail("Copy operation failed: Cancelled")
            ).thenRespond(
                new CopyInResponse(emptySet(), Format.FORMAT_TEXT),
                new CommandComplete("cmd", 1, 1),
                new ReadyForQuery(IDLE)
            ).build();

        new PostgresqlCopyIn(MockContext.builder().client(client).build())
            .copy("some-sql", Mono.just(testPublisher.flux()))
            .as(StepVerifier::create)
            .then(() -> {
                testPublisher.next(byteBuf("a"));
                testPublisher.next(byteBuf("b"));
            })
            .thenCancel()
            .verify();
    }

    private ByteBuf byteBuf(String str) {
        return Unpooled.wrappedBuffer(str.getBytes());
    }

}
