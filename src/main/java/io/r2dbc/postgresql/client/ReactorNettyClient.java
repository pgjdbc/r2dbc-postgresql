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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.postgresql.api.ErrorDetails;
import io.r2dbc.postgresql.api.PostgresqlException;
import io.r2dbc.postgresql.message.backend.BackendKeyData;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.BackendMessageDecoder;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.Field;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.message.backend.NotificationResponse;
import io.r2dbc.postgresql.message.backend.ParameterStatus;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Terminate;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.channel.AbortedException;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;

/**
 * An implementation of client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    static final String CONNECTION_FAILURE = "08006";

    private static final Logger logger = Loggers.getLogger(ReactorNettyClient.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final Supplier<PostgresConnectionClosedException> UNEXPECTED = () -> new PostgresConnectionClosedException("Connection unexpectedly closed");

    private static final Supplier<PostgresConnectionClosedException> EXPECTED = () -> new PostgresConnectionClosedException("Connection closed");

    private final ByteBufAllocator byteBufAllocator;

    private final ConnectionSettings settings;

    private final Connection connection;

    private final Scheduler scheduler;

    private ConnectionContext context;

    private final Sinks.Many<Publisher<FrontendMessage>> requestSink = Sinks.many().unicast().onBackpressureBuffer();

    private final Sinks.Many<NotificationResponse> notificationProcessor = Sinks.many().multicast().directBestEffort();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final BackendMessageSubscriber messageSubscriber = new BackendMessageSubscriber();

    private volatile Integer processId;

    private volatile Integer secretKey;

    private volatile TimeZone timeZone;

    private volatile TransactionStatus transactionStatus = IDLE;

    private volatile Version version = new Version("", 0);

    static {

        // eagerly initialize the scheduler to avoid blocking calls due to TimeZoneDB retrieval
        Schedulers.boundedElastic();
    }

    /**
     * Create a new frame processor connected to a given TCP connection.
     *
     * @param connection the TCP connection
     * @param settings   the connection settings
     * @throws IllegalArgumentException if {@code connection} is {@code null}
     */
    private ReactorNettyClient(Connection connection, ConnectionSettings settings) {
        Assert.requireNonNull(connection, "Connection must not be null");
        this.settings = Assert.requireNonNull(settings, "ConnectionSettings must not be null");

        connection.addHandlerLast(new EnsureSubscribersCompleteChannelHandler(this.requestSink));
        connection.addHandlerLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE - 5, 1, 4, -4, 0));
        this.connection = connection;
        this.byteBufAllocator = connection.outbound().alloc();

        ConnectionContext connectionContext = new ConnectionContext().withChannelId(connection.channel().toString());
        SslHandler sslHandler = this.connection.channel().pipeline().get(SslHandler.class);

        if (sslHandler == null) {
            SSLSessionHandlerAdapter handlerAdapter = this.connection.channel().pipeline().get(SSLSessionHandlerAdapter.class);
            if (handlerAdapter != null) {
                sslHandler = handlerAdapter.getSslHandler();
            }
        }

        if (sslHandler != null) {
            SslHandler toUse = sslHandler;
            connectionContext = connectionContext.withSslSession(() -> toUse.engine().getSession());
        }

        this.context = connectionContext;

        EventLoop eventLoop = connection.channel().eventLoop();
        this.scheduler = Schedulers.fromExecutorService(eventLoop, eventLoop.toString());

        AtomicReference<Throwable> receiveError = new AtomicReference<>();

        connection.inbound().receive()
            .map(BackendMessageDecoder::decode)
            .doOnError(receiveError::set)
            .<BackendMessage>handle((backendMessage, sink) -> {

                if (consumeMessage(backendMessage)) {
                    return;
                }

                sink.next(backendMessage);
            })
            .subscribe(this.messageSubscriber);

        Mono<Void> request = this.requestSink.asFlux()
            .concatMap(Function.identity())
            .flatMap(message -> {
                if (DEBUG_ENABLED) {
                    logger.debug(this.context.getMessage(String.format("Request:  %s", message)));
                }
                return connection.outbound().send(message.encode(this.byteBufAllocator));
            }, 1)
            .then();

        request
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {

            this.notificationProcessor.tryEmitComplete();

            drainError(EXPECTED);

            boolean connected = isConnected();
            if (this.isClosed.compareAndSet(false, true)) {

                if (!connected || this.processId == null) {
                    return closeConnection();
                }

                return Flux.just(Terminate.INSTANCE)
                    .doOnNext(message -> logger.debug(this.context.getMessage(String.format("Request:  %s", message))))
                    .concatMap(message -> this.connection.outbound().send(message.encode(this.connection.outbound().alloc())))
                    .then()
                    .doOnSuccess(v -> this.connection.dispose())
                    .then(this.connection.onDispose());
            }

            return Mono.empty();
        });
    }

    private Mono<? extends Void> closeConnection() {
        this.connection.dispose();
        return this.connection.onDispose();
    }

    @Override
    public Flux<BackendMessage> exchange(Predicate<BackendMessage> takeUntil, Publisher<FrontendMessage> requests) {
        Assert.requireNonNull(takeUntil, "takeUntil must not be null");
        Assert.requireNonNull(requests, "requests must not be null");

        if (!isConnected()) {
            return Flux.error(this.messageSubscriber.createClientClosedException());
        }

        return this.messageSubscriber.addConversation(takeUntil, requests, this::doSendRequest, this::isConnected);
    }

    @Override
    public void send(FrontendMessage message) {
        Assert.requireNonNull(message, "requests must not be null");

        doSendRequest(Mono.just(message));
    }

    private void doSendRequest(Publisher<FrontendMessage> it) {
        this.requestSink.emitNext(it, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    private Mono<Void> resumeError(Throwable throwable) {

        handleConnectionError(throwable);
        this.requestSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);

        if (isSslException(throwable)) {
            logger.debug(this.context.getMessage("Connection Error"), throwable);
        } else {
            logger.warn(this.context.getMessage("Connection Error"), throwable);
        }

        return close();
    }

    private static boolean isSslException(Throwable throwable) {
        return throwable instanceof SSLException || throwable.getCause() instanceof SSLException;
    }

    /**
     * Consume a {@link BackendMessage}. This method can either fully consume the message or it can signal by returning {@code false} that the method wasn't able to fully consume the message and
     * that the message needs to be passed to an active {@link Conversation}.
     *
     * @param message the {@link BackendMessage} to handle
     * @return {@code false} if the message could not be fully consumed and should be propagated to the active {@link Conversation}
     */
    private boolean consumeMessage(BackendMessage message) {

        if (DEBUG_ENABLED) {
            logger.debug(this.context.getMessage(String.format("Response: %s", message)));
        }

        if (message.getClass() == NoticeResponse.class) {
            this.settings.getNoticeLogLevel().log(logger, () -> this.context.getMessage(String.format("Notice: %s", toString(((NoticeResponse) message).getFields()))));
        }

        if (message.getClass() == BackendKeyData.class) {

            BackendKeyData backendKeyData = (BackendKeyData) message;

            this.processId = backendKeyData.getProcessId();
            this.context = this.context.withProcessId(this.processId);
            this.secretKey = backendKeyData.getSecretKey();
            return true;
        }

        if (message.getClass() == ErrorResponse.class) {
            this.settings.getErrorResponseLogLevel().log(logger, () -> String.format("Error: %s", toString(((ErrorResponse) message).getFields())));
        }

        if (message.getClass() == ParameterStatus.class) {
            handleParameterStatus((ParameterStatus) message);
        }

        if (message.getClass() == ReadyForQuery.class) {
            this.transactionStatus = TransactionStatus.valueOf(((ReadyForQuery) message).getTransactionStatus());
        }

        if (message.getClass() == NotificationResponse.class) {
            this.notificationProcessor.tryEmitNext((NotificationResponse) message);
            return true;
        }

        return false;
    }

    private void handleParameterStatus(ParameterStatus message) {

        String name = message.getName();

        if (name.equals("server_version_num") || name.equals("server_version")) {
            Version existingVersion = this.version;

            String versionString = existingVersion.getVersion();
            int versionNum = existingVersion.getVersionNumber();

            if (name.equals("server_version_num")) {
                versionNum = Integer.parseInt(message.getValue());
            }

            if (name.equals("server_version")) {
                versionString = message.getValue();

                if (versionNum == 0) {
                    versionNum = Version.parseServerVersionStr(versionString);
                }
            }

            this.version = new Version(versionString, versionNum);
        }

        if (name.equals("TimeZone")) {
            this.timeZone = TimeZoneUtils.parseBackendTimeZone(message.getValue());
        }
    }

    /**
     * Create a new frame processor connected to a given host.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @throws IllegalArgumentException if {@code host} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(String host, int port) {
        Assert.requireNonNull(host, "host must not be null");

        return connect(host, port, null, new SSLConfig(SSLMode.DISABLE, null, null));
    }

    /**
     * Create a new frame processor connected to a given host.
     *
     * @param host           the host to connect to
     * @param port           the port to connect to
     * @param connectTimeout connect timeout
     * @param sslConfig      SSL configuration
     * @throws IllegalArgumentException if {@code host} is {@code null}
     * @throws IllegalArgumentException if {@code sslConfig} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(String host, int port, @Nullable Duration connectTimeout, SSLConfig sslConfig) {
        Assert.requireNonNull(host, "host must not be null");
        Assert.requireNonNull(sslConfig, "sslConfig must not be null");

        ConnectionSettings.Builder builder = ConnectionSettings.builder().connectTimeout(connectTimeout).sslConfig(sslConfig);

        return connect(InetSocketAddress.createUnresolved(host, port), builder.build());
    }

    /**
     * Create a new frame processor connected to a given {@link SocketAddress}.
     *
     * @param socketAddress the socketAddress to connect to
     * @param settings      the connection settings
     * @throws IllegalArgumentException if {@code socketAddress} or {@code settings} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(SocketAddress socketAddress, ConnectionSettings settings) {
        Assert.requireNonNull(socketAddress, "socketAddress must not be null");
        Assert.requireNonNull(settings, "settings must not be null");

        TcpClient tcpClient = TcpClient.create(settings.getConnectionProvider()).remoteAddress(() -> socketAddress);

        if (settings.hasLoopResources()) {
            tcpClient = tcpClient.runOn(settings.getRequiredLoopResources());
        }

        if (socketAddress instanceof InetSocketAddress) {
            tcpClient = tcpClient.resolver(BalancedResolverGroup.INSTANCE);
            tcpClient = tcpClient.option(ChannelOption.SO_KEEPALIVE, settings.isTcpKeepAlive());
            tcpClient = tcpClient.option(ChannelOption.TCP_NODELAY, settings.isTcpNoDelay());
        }

        if (settings.hasConnectionTimeout()) {
            tcpClient = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.getConnectTimeoutMs());
        }

        return tcpClient.doOnChannelInit((observer, channel, remoteAddress) -> {
            ChannelPipeline pipeline = channel.pipeline();

            InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);
            if (logger.isTraceEnabled()) {
                pipeline.addFirst(LoggingHandler.class.getSimpleName(),
                    new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
            }

            registerSslHandler(socketAddress, settings.getSslConfig(), channel);
        }).connect().flatMap(it ->
            getSslHandshake(it.channel()).thenReturn(new ReactorNettyClient(it, settings))
        );
    }

    private static void registerSslHandler(SocketAddress socketAddress, SSLConfig sslConfig, Channel channel) {
        try {
            if (sslConfig.getSslMode().startSsl()) {

                AbstractPostgresSSLHandlerAdapter sslAdapter;
                if (sslConfig.isDirectSsl()) {
                    sslAdapter = new DirectSSLHandlerAdapter(channel.alloc(), socketAddress, sslConfig);
                } else {
                    sslAdapter = new SSLSessionHandlerAdapter(channel.alloc(), socketAddress, sslConfig);
                }

                channel.pipeline().addFirst(sslAdapter);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Mono<Void> getSslHandshake(Channel channel) {
        AbstractPostgresSSLHandlerAdapter sslAdapter = channel.pipeline().get(AbstractPostgresSSLHandlerAdapter.class);
        return (sslAdapter != null) ? sslAdapter.getHandshake() : Mono.empty();
    }

    @Override
    public Disposable addNotificationListener(Consumer<NotificationResponse> consumer) {
        return this.notificationProcessor.asFlux().subscribe(consumer);
    }

    @Override
    public void addNotificationListener(Subscriber<NotificationResponse> consumer) {
        this.notificationProcessor.asFlux().subscribe(consumer);
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator;
    }

    @Override
    public ConnectionContext getContext() {
        return this.context;
    }

    @Override
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public Optional<Integer> getSecretKey() {
        return Optional.ofNullable(this.secretKey);
    }

    @Override
    public Optional<TimeZone> getTimeZone() {
        return Optional.ofNullable(this.timeZone);
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus;
    }

    @Override
    public Version getVersion() {
        return this.version;
    }

    @Override
    public boolean isConnected() {
        if (this.isClosed.get()) {
            return false;
        }

        Channel channel = this.connection.channel();
        return channel.isOpen();
    }

    @Override
    public Mono<Void> cancelRequest() {
        return Mono.defer(() -> {
            int processId = this.getProcessId().orElseThrow(() -> new IllegalStateException("Connection does not yet have a processId"));
            int secretKey = this.getSecretKey().orElseThrow(() -> new IllegalStateException("Connection does not yet have a secretKey"));

            return ReactorNettyClient.connect(this.connection.channel().remoteAddress(), this.settings)
                .flatMap(client -> CancelRequestMessageFlow.exchange(client, processId, secretKey).then(Mono.defer(client::closeConnection))
                    .onErrorResume(PostgresConnectionClosedException.class::isInstance, e -> Mono.empty()));
        });
    }

    private static String toString(List<Field> fields) {

        StringJoiner joiner = new StringJoiner(", ");
        for (Field field : fields) {
            joiner.add(field.getType().name() + "=" + field.getValue());
        }

        return joiner.toString();
    }

    private void handleClose() {
        if (this.isClosed.compareAndSet(false, true)) {
            drainError(UNEXPECTED);
        } else {
            drainError(EXPECTED);
        }
    }

    private void handleConnectionError(Throwable error) {

        if (AbortedException.isConnectionReset(error) && !isConnected()) {
            drainError(() -> this.messageSubscriber.createClientClosedException(error));
        }

        drainError(() -> new PostgresConnectionException(error));
    }

    private void drainError(Supplier<? extends Throwable> supplier) {

        this.messageSubscriber.close(supplier);

        this.notificationProcessor.tryEmitError(supplier.get());
    }

    private final class EnsureSubscribersCompleteChannelHandler extends ChannelDuplexHandler {

        private final Sinks.Many<?> requestSink;

        private EnsureSubscribersCompleteChannelHandler(Sinks.Many<?> requestSink) {
            this.requestSink = requestSink;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);

            this.requestSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            handleClose();
        }

    }

    static class PostgresConnectionClosedException extends R2dbcNonTransientResourceException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        public PostgresConnectionClosedException(String reason) {
            super(reason, CONNECTION_FAILURE, 0, (String) null);
            this.errorDetails = ErrorDetails.fromCodeAndMessage(CONNECTION_FAILURE, reason);
        }

        public PostgresConnectionClosedException(String reason, @Nullable Throwable cause) {
            super(reason, CONNECTION_FAILURE, 0, null, cause);
            this.errorDetails = ErrorDetails.fromCodeAndMessage(CONNECTION_FAILURE, reason);
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException implements PostgresqlException {

        private final static ErrorDetails ERROR_DETAILS = ErrorDetails.fromCodeAndMessage(CONNECTION_FAILURE, "An I/O error occurred while sending to the backend or receiving from the backend");

        public PostgresConnectionException(Throwable cause) {
            super(ERROR_DETAILS.getMessage(), ERROR_DETAILS.getCode(), 0, null, cause);
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return ERROR_DETAILS;
        }

    }

    static class RequestQueueException extends R2dbcTransientResourceException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        public RequestQueueException(String message) {
            super(message, CONNECTION_FAILURE, 0, (String) null);
            this.errorDetails = ErrorDetails.fromCodeAndMessage(CONNECTION_FAILURE, message);
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    static class ResponseQueueException extends R2dbcNonTransientResourceException implements PostgresqlException {

        private final ErrorDetails errorDetails;

        public ResponseQueueException(String message) {
            super(message, CONNECTION_FAILURE, 0, (String) null);
            this.errorDetails = ErrorDetails.fromCodeAndMessage(CONNECTION_FAILURE, message);
        }

        @Override
        public ErrorDetails getErrorDetails() {
            return this.errorDetails;
        }

    }

    /**
     * Value object representing a single conversation. The driver permits a single conversation at a time to ensure that request messages get routed to the proper response receiver and do not leak
     * into other conversations. A conversation must be finished in the sense that the {@link Publisher} of {@link FrontendMessage} has completed before the next conversation is started.
     * <p>
     * A single conversation can make use of pipelining.
     */
    private static class Conversation {

        private static final AtomicLongFieldUpdater<Conversation> DEMAND_UPDATER = AtomicLongFieldUpdater.newUpdater(Conversation.class, "demand");

        private final Predicate<BackendMessage> takeUntil;

        private final FluxSink<BackendMessage> sink;

        // access via DEMAND_UPDATER
        private volatile long demand;

        private Conversation(Predicate<BackendMessage> takeUntil, FluxSink<BackendMessage> sink) {
            this.sink = sink;
            this.takeUntil = takeUntil;
        }

        private void decrementDemand() {
            Operators.addCap(DEMAND_UPDATER, this, -1);
        }

        /**
         * Check whether the {@link BackendMessage} can complete the conversation.
         *
         * @param item the message to test whether it can complete the current conversation
         * @return whether the {@link BackendMessage} can complete the current conversation
         */
        public boolean canComplete(BackendMessage item) {
            return this.takeUntil.test(item);
        }

        /**
         * Complete the conversation.
         *
         * @param item the message completing the conversation
         */
        public void complete(BackendMessage item) {

            ReferenceCountUtil.release(item);
            if (!this.sink.isCancelled()) {
                this.sink.complete();
            }
        }

        /**
         * Emit a {@link BackendMessage}.
         *
         * @param item the item to emit
         */
        public void emit(BackendMessage item) {

            if (this.sink.isCancelled()) {
                ReferenceCountUtil.release(item);
            }

            decrementDemand();
            this.sink.next(item);
        }

        /**
         * Notify the conversation about an error. Drops errors silently if the conversation is finished.
         *
         * @param throwable the error signal
         */
        public void onError(Throwable throwable) {

            if (!this.sink.isCancelled()) {
                this.sink.error(throwable);
            }
        }

        public boolean hasDemand() {
            return DEMAND_UPDATER.get(this) > 0;
        }

        public boolean isCancelled() {
            return this.sink.isCancelled();
        }

        public void incrementDemand(long n) {
            Operators.addCap(DEMAND_UPDATER, this, n);
        }

    }

    /**
     * Subscriber that handles {@link Conversation}s and keeps track of the current demand. It also routes {@link BackendMessage}s to the currently active {@link Conversation}.
     */
    private class BackendMessageSubscriber implements CoreSubscriber<BackendMessage> {

        private static final int DEMAND = 256;

        private final Queue<Conversation> conversations = Queues.<Conversation>small().get();

        private final Queue<BackendMessage> buffer = Queues.<BackendMessage>get(DEMAND).get();

        private final AtomicLong demand = new AtomicLong(0);

        private final AtomicBoolean drain = new AtomicBoolean();

        private volatile boolean terminated;

        private Subscription upstream;

        public Flux<BackendMessage> addConversation(Predicate<BackendMessage> takeUntil, Publisher<FrontendMessage> requests, Consumer<Publisher<FrontendMessage>> sender,
                                                    Supplier<Boolean> isConnected) {

            return Flux.create(sink -> {

                Conversation conversation = new Conversation(takeUntil, sink);

                // ensure ordering in which conversations are added to both queues.
                synchronized (this.conversations) {
                    if (this.conversations.offer(conversation)) {

                        sink.onRequest(value -> onRequest(conversation, value));

                        if (!isConnected.get()) {
                            sink.error(createClientClosedException());
                            return;
                        }

                        sender.accept(requests);
                    } else {
                        sink.error(new RequestQueueException("Cannot exchange messages because the request queue limit is exceeded"));
                    }
                }
            });
        }

        PostgresConnectionClosedException createClientClosedException() {
            return createClientClosedException(null);
        }

        PostgresConnectionClosedException createClientClosedException(@Nullable Throwable cause) {
            return new PostgresConnectionClosedException("Cannot exchange messages because the connection is closed", cause);
        }

        /**
         * {@link Subscription#request(long)} callback. Request more for a {@link Conversation}. Potentially, demands also more upstream elements.
         *
         * @param conversation the conversation subject
         * @param n            number of requested elements
         */
        public void onRequest(Conversation conversation, long n) {
            conversation.incrementDemand(n);
            demandMore();
            tryDrainLoop();
        }

        /**
         * {@link Subscriber#onSubscribe(Subscription)} callback. Registers the {@link Subscription} and potentially requests more upstream elements.
         *
         * @param s the subscription
         */
        @Override
        public void onSubscribe(Subscription s) {
            this.upstream = s;
            demandMore();
        }

        /**
         * {@link Subscriber#onNext(Object)} callback. Decrements upstream demand and attempts to emit {@link BackendMessage} to an active {@link Conversation}. If a conversation has no demand, it
         * will be buffered.
         *
         * @param message the message to emit
         */
        @Override
        public void onNext(BackendMessage message) {

            if (this.terminated) {
                ReferenceCountUtil.release(message);
                Operators.onNextDropped(message, currentContext());
                return;
            }

            this.demand.decrementAndGet();

            // fast-path
            if (this.buffer.isEmpty()) {
                Conversation conversation = this.conversations.peek();
                if (conversation != null && conversation.hasDemand()) {
                    emit(conversation, message);
                    potentiallyDemandMore(conversation);
                    return;
                }
            }

            // slow-path
            if (!this.buffer.offer(message)) {
                ReferenceCountUtil.release(message);
                Operators.onNextDropped(message, currentContext());
                onError(new ResponseQueueException("Response queue is full"));
                return;
            }

            tryDrainLoop();
        }

        /**
         * {@link Subscriber#onError(Throwable)} callback.
         *
         * @param throwable the error to emit
         */
        @Override
        public void onError(Throwable throwable) {

            if (this.terminated) {
                Operators.onErrorDropped(throwable, currentContext());
                return;
            }

            handleConnectionError(throwable);
            ReactorNettyClient.this.requestSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            this.terminated = true;

            if (isSslException(throwable)) {
                logger.debug(ReactorNettyClient.this.context.getMessage("Connection Error"), throwable);
            } else {
                logger.error(ReactorNettyClient.this.context.getMessage("Connection Error"), throwable);
            }

            ReactorNettyClient.this.close().subscribe();
        }

        /**
         * {@link Subscriber#onComplete()} callback.
         */
        @Override
        public void onComplete() {
            this.terminated = true;
            ReactorNettyClient.this.handleClose();
        }

        /**
         * Context propagation from an active {@link Conversation}.
         */
        @Override
        public Context currentContext() {
            Conversation receiver = this.conversations.peek();
            return receiver != null ? Context.of(receiver.sink.contextView()) : Context.empty();
        }

        private void tryDrainLoop() {
            while (hasBufferedItems() && hasDownstreamDemand()) {
                if (!drainLoop()) {
                    return;
                }
            }
        }

        /**
         * Drains the buffer. Guarded for single-thread access.
         *
         * @return {@code true} if the drain loop was entered successfully. {@code false} otherwise (i.e. a different thread already works on the drain loop).
         */
        private boolean drainLoop() {

            if (!this.drain.compareAndSet(false, true)) {
                return false;
            }

            Conversation lastConversation = null;

            try {

                while (hasBufferedItems()) {

                    Conversation conversation = this.conversations.peek();
                    lastConversation = conversation;
                    if (conversation == null) {
                        break;
                    }

                    if (conversation.hasDemand()) {

                        BackendMessage item = this.buffer.poll();

                        if (item == null) {
                            break;
                        }

                        emit(conversation, item);
                    } else {
                        break;
                    }
                }

            } finally {
                this.drain.compareAndSet(true, false);
            }

            potentiallyDemandMore(lastConversation);

            return true;
        }

        private void potentiallyDemandMore(@Nullable Conversation lastConversation) {
            if (lastConversation == null || lastConversation.hasDemand() || lastConversation.isCancelled()) {
                demandMore();
            }
        }

        private void emit(Conversation conversation, BackendMessage item) {
            if (conversation.canComplete(item)) {
                this.conversations.poll();
                conversation.complete(item);
            } else {
                conversation.emit(item);
            }
        }

        private void demandMore() {
            if (!hasBufferedItems() && this.demand.compareAndSet(0, DEMAND)) {
                this.upstream.request(DEMAND);
            }
        }

        private boolean hasDownstreamDemand() {

            Conversation conversation = this.conversations.peek();

            return conversation != null && conversation.hasDemand();
        }

        private boolean hasBufferedItems() {
            return !this.buffer.isEmpty();
        }

        /**
         * Cleanup the subscriber by terminating all {@link Conversation}s and purging the data buffer. All conversations are completed with an error signal provided by {@code supplier}.
         *
         * @param supplier the error supplier
         */
        public void close(Supplier<? extends Throwable> supplier) {

            this.terminated = true;
            Conversation receiver;

            while ((receiver = this.conversations.poll()) != null) {
                receiver.onError(supplier.get());
            }

            while (!this.buffer.isEmpty()) {
                ReferenceCountUtil.release(this.buffer.poll());
            }
        }

    }

}
