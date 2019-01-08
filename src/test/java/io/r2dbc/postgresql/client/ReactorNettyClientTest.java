/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.Disposable;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ReactorNettyClientTest {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    private final ReactorNettyClient client = ReactorNettyClient.connect(SERVER.getHost(), SERVER.getPort())
        .delayUntil(client -> StartupMessageFlow
            .exchange(this.getClass().getName(), m -> new PasswordAuthenticationHandler(SERVER.getPassword(), SERVER.getUsername()), client, SERVER.getDatabase(), SERVER.getUsername()))
        .block();

    @Test
    void close() {
        this.client.close()
            .thenMany(this.client.exchange(Mono.empty()))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(IllegalStateException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @AfterEach
    void closeClient() {
        this.client.close()
            .block();
    }

    @Test
    void constructorNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReactorNettyClient.connect(null, SERVER.getPort()))
            .withMessage("host must not be null");
    }

    @BeforeEach
    void createTable() {
        SERVER.getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
    }

    @AfterEach
    void dropTable() {
        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

    @Test
    void exchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        this.client
            .exchange(Mono.just(new Query("SELECT value FROM test")))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .verifyComplete();
    }

    @Test
    void exchangeNoPublisher() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.client.exchange(null))
            .withMessage("requests must not be null");
    }

    @Test
    void handleBackendData() {
        assertThat(this.client.getProcessId()).isNotEmpty();
        assertThat(this.client.getSecretKey()).isNotEmpty();
    }

    @Test
    void handleTransactionStatus() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.client
            .exchange(Mono.just(new Query("BEGIN")))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.OPEN);
    }

    @Test
    void largePayload() {
        IntStream.range(0, 1_000)
            .forEach(i -> SERVER.getJdbcOperations().update("INSERT INTO test VALUES(?)", i));

        this.client
            .exchange(Mono.just(new Query("SELECT value FROM test")))
            .as(StepVerifier::create)
            .expectNextCount(1 + 1_000 + 1)
            .verifyComplete();
    }

    @Test
    void parallelExchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        this.client
            .exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1")))
            .zipWith(this.client.exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1 OFFSET 1"))))
            .flatMapIterable(t -> Arrays.asList(t.getT1(), t.getT2()))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .expectNext(new CommandComplete("SELECT", null, 1))
            .verifyComplete();
    }

    @Test
    void handleNotify() {
        UnicastProcessor<NotificationResponse> response = UnicastProcessor.create();
        this.client.addNotificationListener(response::onNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .blockLast();

        SERVER.getJdbcOperations().execute("NOTIFY events, 'test'");

        StepVerifier.create(response)
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("test"))
            .thenCancel()
            .verify();
    }

    @Test
    void handleTrigger() {
        SERVER.getJdbcOperations().execute(
            "CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$\n" +
                "  DECLARE\n" +
                "    payload JSON;\n" +
                "  BEGIN\n" +
                "    payload = row_to_json(NEW);\n" +
                "    PERFORM pg_notify('events', payload::text);\n" +
                "    RETURN NULL;\n" +
                "  END;\n" +
                "$$ LANGUAGE plpgsql;");

        SERVER.getJdbcOperations().execute(
            "CREATE TRIGGER notify_test_event\n" +
                "AFTER INSERT OR UPDATE OR DELETE ON test\n" +
                "  FOR EACH ROW EXECUTE PROCEDURE notify_event();");

        UnicastProcessor<NotificationResponse> response = UnicastProcessor.create();
        this.client.addNotificationListener(response::onNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .blockLast();

        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        StepVerifier.create(response)
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":100}"))
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":1000}"))
            .thenCancel()
            .verify();
    }
}
