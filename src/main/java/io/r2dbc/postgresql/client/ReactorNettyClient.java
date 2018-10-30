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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.r2dbc.postgresql.message.backend.BackendKeyData;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.BackendMessageDecoder;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.message.backend.ParameterStatus;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.concurrent.Queues;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.util.PredicateUtils.not;

/**
 * An implementation of client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicReference<ByteBufAllocator> byteBufAllocator = new AtomicReference<>();

    private final AtomicReference<Connection> connection = new AtomicReference<>();

    private final BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleErrorResponse = handleBackendMessage(ErrorResponse.class,
        (message, sink) -> {
            this.logger.error("Error: {}", toString(message.getFields()));
            sink.next(message);
        });

    private final BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleNoticeResponse = handleBackendMessage(NoticeResponse.class,
        (message, sink) -> this.logger.warn("Notice: {}", toString(message.getFields())));

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final ConcurrentMap<String, String> parameterStatus = new ConcurrentHashMap<>();

    private final BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleParameterStatus = handleBackendMessage(ParameterStatus.class,
        (message, sink) -> this.parameterStatus.put(message.getName(), message.getValue()));

    private final AtomicReference<Integer> processId = new AtomicReference<>();

    private final EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<FrontendMessage> requests = this.requestProcessor.sink();

    private final Queue<MonoSink<Flux<BackendMessage>>> responseReceivers = Queues.<MonoSink<Flux<BackendMessage>>>unbounded().get();

    private final AtomicReference<Integer> secretKey = new AtomicReference<>();

    private final BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleBackendKeyData = handleBackendMessage(BackendKeyData.class,
        (message, sink) -> {
            this.processId.set(message.getProcessId());
            this.secretKey.set(message.getSecretKey());
        });

    private final AtomicReference<TransactionStatus> transactionStatus = new AtomicReference<>(IDLE);

    private final BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleReadyForQuery = handleBackendMessage(ReadyForQuery.class,
        (message, sink) -> {
            this.transactionStatus.set(TransactionStatus.valueOf(message.getTransactionStatus()));
            sink.next(message);
        });

    /**
     * Creates a new frame processor connected to a given TCP connection.
     *
     * @param connection the TCP connection
     * @throws NullPointerException if {@code connection} is {@code null}
     */
    private ReactorNettyClient(Connection connection) {
        Objects.requireNonNull(connection, "Connection must not be null");

        connection.addHandler(new EnsureSubscribersCompleteChannelHandler(this.requestProcessor, this.responseReceivers));

        BackendMessageDecoder decoder = new BackendMessageDecoder();

        this.byteBufAllocator.set(connection.outbound().alloc());

        connection.inbound().receive()
            .retain()
            .concatMap(decoder::decode)
            .doOnNext(message -> this.logger.debug("Response: {}", message))
            .handle(this.handleNoticeResponse)
            .handle(this.handleErrorResponse)
            .handle(this.handleBackendKeyData)
            .handle(this.handleParameterStatus)
            .handle(this.handleReadyForQuery)
            .windowWhile(not(ReadyForQuery.class::isInstance))
            .doOnNext(fluxOfMessages -> this.responseReceivers.poll().success(fluxOfMessages))
            .doOnComplete(() -> this.responseReceivers.poll().success(Flux.empty()))
            .subscribe();

        this.requestProcessor
            .doOnNext(message -> this.logger.debug("Request:  {}", message))
            .concatMap(message -> connection.outbound().send(message.encode(connection.outbound().alloc())))
            .subscribe();

        this.connection.set(connection);
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @throws NullPointerException if {@code host} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(String host, int port) {
        Objects.requireNonNull(host, "host must not be null");

        return connect(ConnectionProvider.newConnection(), host, port);
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param connectionProvider the connection provider resources
     * @param host               the host to connect to
     * @param port               the port to connect to
     * @throws NullPointerException if {@code host} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(ConnectionProvider connectionProvider, String host, int port) {
        Objects.requireNonNull(connectionProvider, "connectionProvider must not be null");
        Objects.requireNonNull(host, "host must not be null");

        Mono<? extends Connection> connection = TcpClient.create(connectionProvider)
            .host(host).port(port)
            .connect();

        return connection.map(ReactorNettyClient::new);
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            Connection connection = this.connection.getAndSet(null);

            if (connection == null) {
                return Mono.empty();
            }

            return TerminationMessageFlow.exchange(this)
                .doOnComplete(() -> {
                    connection.disposeNow();
                    this.isClosed.set(true);
                })
                .then();
        });
    }

    @Override
    public Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests) {
        Objects.requireNonNull(requests, "requests must not be null");

        return Mono
            .<Flux<BackendMessage>>create(sink -> {
                if (this.isClosed.get()) {
                    sink.error(new IllegalStateException("Cannot exchange messages because the connection is closed"));
                }

                final AtomicInteger once = new AtomicInteger();

                Flux.from(requests)
                    .subscribe(message -> {
                        if (once.get() == 0 && once.compareAndSet(0, 1)) {
                            synchronized (this) {
                                this.responseReceivers.add(sink);
                                this.requests.next(message);
                            }
                            return;
                        }

                        this.requests.next(message);
                    }, this.requests::error);

            })
            .flatMapMany(Function.identity());
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator.get();
    }

    @Override
    public Map<String, String> getParameterStatus() {
        return new HashMap<>(this.parameterStatus);
    }

    @Override
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId.get());
    }

    @Override
    public Optional<Integer> getSecretKey() {
        return Optional.ofNullable(this.secretKey.get());
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus.get();
    }

    @SuppressWarnings("unchecked")
    private static <T extends BackendMessage> BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleBackendMessage(Class<T> type, BiConsumer<T, SynchronousSink<BackendMessage>> consumer) {
        return (message, sink) -> {
            if (type.isInstance(message)) {
                consumer.accept((T) message, sink);
            } else {
                sink.next(message);
            }
        };
    }

    private static String toString(List<Field> fields) {
        return fields.stream()
            .map(field -> String.format("%s=%s", field.getType().name(), field.getValue()))
            .collect(Collectors.joining(", "));
    }

    private static final class EnsureSubscribersCompleteChannelHandler extends ChannelDuplexHandler {

        private final EmitterProcessor<FrontendMessage> requestProcessor;

        private final Queue<MonoSink<Flux<BackendMessage>>> responseReceivers;

        private EnsureSubscribersCompleteChannelHandler(EmitterProcessor<FrontendMessage> requestProcessor, Queue<MonoSink<Flux<BackendMessage>>> responseReceivers) {
            this.requestProcessor = requestProcessor;
            this.responseReceivers = responseReceivers;
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);

            this.requestProcessor.onComplete();

            for (MonoSink<Flux<BackendMessage>> responseReceiver = this.responseReceivers.poll(); responseReceiver != null; responseReceiver = this.responseReceivers.poll()) {
                responseReceiver.success(Flux.empty());
            }
        }
    }

}
