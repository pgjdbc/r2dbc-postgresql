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

package io.r2dbc.postgresql.client;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.DataRow;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Query;
import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.test.StepVerifier;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for {@link ReactorNettyClient}.
 */
final class ReactorNettyClientIntegrationTests {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    static final Field CONNECTION = ReflectionUtils.findField(ReactorNettyClient.class, "connection");

    static {
        ReflectionUtils.makeAccessible(CONNECTION);
    }

    private final ReactorNettyClient client = ReactorNettyClient.connect(SERVER.getHost(), SERVER.getPort())
        .delayUntil(client -> StartupMessageFlow
            .exchange(this.getClass().getName(), m -> new PasswordAuthenticationHandler(SERVER.getPassword(), SERVER.getUsername()), client, SERVER.getDatabase(), SERVER.getUsername()))
        .block();

    @BeforeEach
    void before() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
    }

    @AfterEach
    void after() {
        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        this.client.close()
            .block();
    }

    @Test
    @Timeout(50)
    void concurrentConsumptionShouldComplete() {

        IntStream.range(0, 1000)
            .forEach(i -> SERVER.getJdbcOperations().update("INSERT INTO test VALUES(?),(?),(?),(?),(?),(?),(?),(?),(?),(?)", i, i, i, i, i, i, i, i, i, i));

        this.client
            .exchange(Mono.just(new Query("SELECT value FROM test")))
            .doOnNext(ReferenceCountUtil::release)
            .limitRate(1)
            .publishOn(Schedulers.parallel())
            .as(StepVerifier::create)
            .thenConsumeWhile((t) -> true)
            .verifyComplete();
    }

    @Test
    void close() {
        this.client.close()
            .thenMany(this.client.exchange(Mono.empty()))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @Test
    void disconnectedShouldRejectExchange() {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);
        connection.channel().close().awaitUninterruptibly();

        this.client.close()
            .thenMany(this.client.exchange(Mono.empty()))
            .as(StepVerifier::create)
            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(R2dbcNonTransientResourceException.class).hasMessage("Cannot exchange messages because the connection is closed"));
    }

    @Test
    void shouldCancelExchangeOnCloseFirstMessage() throws Exception {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);

        Sinks.Many<FrontendMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        Flux<BackendMessage> query = this.client.exchange(messages.asFlux());
        CompletableFuture<List<BackendMessage>> future = query.collectList().toFuture();

        connection.channel().eventLoop().execute(() -> {

            connection.channel().close();
            messages.tryEmitNext(new Query("SELECT value FROM test"));
        });

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected PostgresConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(ReactorNettyClient.PostgresConnectionClosedException.class).hasMessageContaining("Cannot exchange messages");
        }
    }

    @Test
    void shouldCancelExchangeOnCloseInFlight() throws Exception {

        Connection connection = (Connection) ReflectionUtils.getField(CONNECTION, this.client);

        Sinks.Many<FrontendMessage> messages = Sinks.many().unicast().onBackpressureBuffer();
        Flux<BackendMessage> query = this.client.exchange(messages.asFlux());
        CompletableFuture<List<BackendMessage>> future = query.doOnNext(ignore -> {
            connection.channel().close();
            messages.tryEmitNext(new Query("SELECT value FROM test"));

        }).collectList().toFuture();

        messages.tryEmitNext(new Query("SELECT value FROM test;SELECT value FROM test;"));

        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected PostgresConnectionClosedException");
        } catch (ExecutionException e) {
            assertThat(e).hasCauseInstanceOf(ReactorNettyClient.PostgresConnectionClosedException.class);
        }
    }

    @Test
    void constructorNoHost() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReactorNettyClient.connect(null, SERVER.getPort()))
            .withMessage("host must not be null");
    }

    @Test
    void exchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1L))
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
    void handleParameterStatus() {
        Version version = this.client.getVersion();
        assertThat(version.getVersion()).isNotEmpty();
        assertThat(version.getVersionNumber()).isNotZero();
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
    void handleTransactionStatusAfterCommand() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);
    }

    @Test
    void handleTransactionStatusAfterCommit() {
        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);

        this.client
            .exchange(Mono.just(new Query("BEGIN")))
            .blockLast();

        this.client
            .exchange(Mono.just(new Query("COMMIT")))
            .blockLast();

        assertThat(this.client.getTransactionStatus()).isEqualTo(TransactionStatus.IDLE);
    }

    @Test
    void largePayload() {
        IntStream.range(0, 1_000)
            .forEach(i -> SERVER.getJdbcOperations().update("INSERT INTO test VALUES(?)", i));

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test"))))
            .as(StepVerifier::create)
            .expectNextCount(1 + 1_000 + 1)
            .verifyComplete();
    }

    @Test
    void parallelExchange() {
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        this.datarowCleanup(this.client
            .exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1"))))
            .zipWith(this.datarowCleanup(this.client.exchange(Mono.just(new Query("SELECT value FROM test LIMIT 1 OFFSET 1")))))
            .flatMapIterable(t -> Arrays.asList(t.getT1(), t.getT2()))
            .as(StepVerifier::create)
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(RowDescription.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .assertNext(message -> assertThat(message).isInstanceOf(DataRow.class))
            .expectNext(new CommandComplete("SELECT", null, 1L))
            .expectNext(new CommandComplete("SELECT", null, 1L))
            .verifyComplete();
    }

    @Test
    void timeoutTest() {
        PostgresqlConnectionFactory postgresqlConnectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host("1.2.3.4")
            .port(81)
            .username("test")
            .password("test")
            .database(SERVER.getDatabase())
            .applicationName(ReactorNettyClientIntegrationTests.class.getName())
            .connectTimeout(Duration.ofMillis(1))
            .build());

        postgresqlConnectionFactory.create()
            .as(StepVerifier::create)
            .expectError(R2dbcNonTransientResourceException.class)
            .verify(Duration.ofMillis(500));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void unixDomainSocketTest() {

        String socket = "/tmp/.s.PGSQL.5432";

        assumeThat(KQueue.isAvailable() || Epoll.isAvailable()).describedAs("EPoll or KQueue must be available").isTrue();
        assumeThat(new File(socket)).exists();

        PostgresqlConnectionFactory postgresqlConnectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .socket(socket)
            .username("postgres")
            .database(SERVER.getDatabase())
            .applicationName(ReactorNettyClientIntegrationTests.class.getName())
            .build());

        postgresqlConnectionFactory.create()
            .flatMapMany(it -> {
                return it.createStatement("SELECT 1").execute().flatMap(r -> r.map((row, rowMetadata) -> row.get(0))).concatWith(it.close());
            })
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    @Timeout(10)
    void queryNeverCompletes() {
        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host(SERVER.getHost())
            .port(SERVER.getPort())
            .username(SERVER.getUsername())
            .password(SERVER.getPassword())
            .database(SERVER.getDatabase())
            .applicationName(ReactorNettyClientIntegrationTests.class.getName())
            .build());
        Flux.usingWhen(
            connectionFactory.create(),
            connection -> connection.createStatement("SELECT floor(random() * 100) FROM generate_series(1, 256 + 15)").execute()
                .limitRate(1, 1)
                .flatMap(r -> r.map((row, meta) -> row.get(0)).limitRate(1, 1))
                .limitRate(1, 1)
                .delayElements(Duration.of(1, ChronoUnit.MILLIS))
                .limitRate(1, 1),
            io.r2dbc.spi.Connection::close
        )
            .as(StepVerifier::create)
            .thenAwait()
            .expectNextCount(256 + 15)
            .verifyComplete();
    }

    public static class FailedVerification implements HostnameVerifier {

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return false;
        }

    }

    public static class NoVerification implements HostnameVerifier {

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }

    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    final class ScramIntegrationTests {

        @Test
        void scramAuthentication() {
            createConnectionFactory("test-scram", "test-scram").create()
                .flatMapMany(c -> c.createStatement("SELECT 1 test").execute())
                .flatMap(r -> r.map((row, meta) -> row.get(0)))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();
        }

        @Test
        void scramAuthenticationFailed() {
            createConnectionFactory("test-scram", "wrong").create()
                .as(StepVerifier::create)
                .verifyError(R2dbcPermissionDeniedException.class);
        }

        private PostgresqlConnectionFactory createConnectionFactory(String username, String password) {
            return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
                .host(SERVER.getHost())
                .port(SERVER.getPort())
                .username(username)
                .password(password)
                .database(SERVER.getDatabase())
                .applicationName(ReactorNettyClientIntegrationTests.class.getName())
                .build());
        }

    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    final class SslIntegrationTests {

        @Test
        void exchangeSslWithClientCert() {
            client(
                c -> c,
                c -> c.map(client -> client.createStatement("SELECT 10")
                    .execute()
                    .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
                    .as(StepVerifier::create)
                    .expectNext(10)
                    .verifyComplete()));
        }

        @Test
        void exchangeSslWithClientCertInvalidClientCert() {
            client(
                c -> c.sslRootCert(SERVER.getServerCrt())
                    .sslCert(SERVER.getServerCrt())
                    .sslKey(SERVER.getServerKey()),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcPermissionDeniedException.class)
                    .verify());
        }

        @Test
        void exchangeSslWithClientCertNoCert() {
            client(
                c -> c
                    .password("test"),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcPermissionDeniedException.class));
        }

        @Test
        void exchangeSslWithPassword() {
            client(
                c -> c
                    .sslRootCert(SERVER.getServerCrt())
                    .username("test-ssl")
                    .password("test-ssl"),
                c -> c.map(client -> client.createStatement("SELECT 10")
                    .execute()
                    .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
                    .as(StepVerifier::create)
                    .expectNext(10)
                    .verifyComplete()));
        }

        @Test
        void invalidServerCertificate() {
            client(
                c -> c
                    .sslRootCert(SERVER.getClientCrt()),
                c -> c
                    .as(StepVerifier::create)
                    .expectError(R2dbcNonTransientResourceException.class)
                    .verify());
        }

        @Test
        void userIsNotAllowedToLoginWithSsl() {
            client(
                c -> c.sslRootCert(SERVER.getServerCrt())
                    .username(SERVER.getUsername())
                    .password(SERVER.getPassword())
                ,
                c -> c
                    .as(StepVerifier::create)
                    .verifyError(R2dbcPermissionDeniedException.class));
        }

        @Test
        void userIsNotAllowedToLoginWithoutSsl() {
            client(
                c -> c.sslMode(SSLMode.DISABLE)
                    .username("test-ssl")
                    .password("test-ssl"),
                c -> c
                    .as(StepVerifier::create)
                    .verifyError(R2dbcPermissionDeniedException.class));
        }

        private void client(Function<PostgresqlConnectionConfiguration.Builder, PostgresqlConnectionConfiguration.Builder> configurer,
                            Consumer<Mono<PostgresqlConnection>> connectionConsumer) {
            PostgresqlConnectionConfiguration.Builder defaultConfig = PostgresqlConnectionConfiguration.builder()
                .database(SERVER.getDatabase())
                .host(SERVER.getHost())
                .port(SERVER.getPort())
                .username("test-ssl-with-cert")
                .password((String) null)
                .sslMode(SSLMode.VERIFY_FULL)
                .sslHostnameVerifier(new NoVerification());
            new PostgresqlConnectionFactory(configurer.apply(defaultConfig).build()).create()
                .onErrorResume(e -> Mono.fromRunnable(() -> connectionConsumer.accept(Mono.error(e))))
                .delayUntil(connection -> Mono.fromRunnable(() -> connectionConsumer.accept(Mono.just(connection))))
                .flatMap(PostgresqlConnection::close)
                .block();
        }

        @Nested
        class SslModes {

            @Test
            void allowFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void allowFallbackToSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void allowFallbackToSslAndFailedAgain() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username("test-ssl-test")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void allowWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.ALLOW)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void disabled() {
                client(
                    c -> c
                        .sslMode(SSLMode.DISABLE)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void disabledFailedForSslOnlyUser() {
                client(
                    c -> c
                        .sslMode(SSLMode.DISABLE)
                        .password("test-ssl")
                        .password("test-ssl")
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectError(R2dbcPermissionDeniedException.class)
                        .verify());
            }

            @Test
            void preferFallbackToTcp() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void preferFallbackToTcpAndFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .username("test-ssl-test")
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .expectError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void preferWithSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.PREFER)
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void require() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void requireConnectsWithoutCertificate() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void requireFailed() {
                client(
                    c -> c
                        .sslMode(SSLMode.REQUIRE)
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword())
                    ,
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyCa() {
                client(
                    c -> c.sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt())
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyCaWithCustomizer() {
                client(
                    c -> c.sslContextBuilderCustomizer(sslContextBuilder -> {
                        return sslContextBuilder.trustManager(new File(SERVER.getServerCrt()))
                            .keyManager(new File(SERVER.getClientCrt()), new File(SERVER.getClientKey()));
                    })
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyCaFailedWithWrongRootCert() {
                client(
                    c -> c
                        .sslRootCert(SERVER.getClientCrt())
                        .sslMode(SSLMode.VERIFY_CA),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcNonTransientResourceException.class));
            }

            @Test
            void verifyCaFailedWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_CA)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt())
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyFull() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getServerCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .expectNextCount(1)
                        .verifyComplete());
            }

            @Test
            void verifyFullFailedWithWrongHost() {
                client(
                    c -> c.sslRootCert(SERVER.getServerCrt())
                        .sslHostnameVerifier(new FailedVerification())
                        .sslMode(SSLMode.VERIFY_FULL),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

            @Test
            void verifyFullFailedWithWrongRootCert() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getClientCrt())
                        .sslKey(SERVER.getClientKey())
                        .sslCert(SERVER.getClientCrt()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcNonTransientResourceException.class));
            }

            @Test
            void verifyFullFailedWithoutSsl() {
                client(
                    c -> c
                        .sslMode(SSLMode.VERIFY_FULL)
                        .sslRootCert(SERVER.getServerCrt())
                        .username(SERVER.getUsername())
                        .password(SERVER.getPassword()),
                    c -> c
                        .as(StepVerifier::create)
                        .verifyError(R2dbcPermissionDeniedException.class));
            }

        }

    }

    @Test
    void handleNotify() {
        Sinks.Many<NotificationResponse> sink = Sinks.many().unicast().onBackpressureBuffer();
        this.client.addNotificationListener(sink::tryEmitNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .blockLast();

        SERVER.getJdbcOperations().execute("NOTIFY events, 'test'");

        StepVerifier.create(sink.asFlux())
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

        Sinks.Many<NotificationResponse> sink = Sinks.many().unicast().onBackpressureBuffer();
        this.client.addNotificationListener(sink::tryEmitNext);

        this.client
            .exchange(Mono.just(new Query("LISTEN events")))
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES (1000)");

        StepVerifier.create(sink.asFlux())
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":100}"))
            .assertNext(message -> assertThat(message.getPayload()).isEqualTo("{\"value\":1000}"))
            .thenCancel()
            .verify();
    }

    private Flux<BackendMessage> datarowCleanup(Flux<BackendMessage> in) {
        return Flux.create(sink -> Flux.from(in).subscribe(message -> {
            sink.next(message);
            ReferenceCountUtil.release(message);
        }, sink::error, sink::complete));
    }

}
