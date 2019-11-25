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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
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
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.client.TransactionStatus.IDLE;

/**
 * An implementation of client based on the Reactor Netty project.
 *
 * @see TcpClient
 */
public final class ReactorNettyClient implements Client {

    private static final Logger logger = Loggers.getLogger(ReactorNettyClient.class);

    private static final boolean DEBUG_ENABLED = logger.isDebugEnabled();

    private static final Supplier<PostgresConnectionClosedException> UNEXPECTED = () -> new PostgresConnectionClosedException("Connection unexpectedly closed");

    private static final Supplier<PostgresConnectionClosedException> EXPECTED = () -> new PostgresConnectionClosedException("Connection closed");

    private final ByteBufAllocator byteBufAllocator;

    private final Connection connection;

    private final EmitterProcessor<FrontendMessage> requestProcessor = EmitterProcessor.create(false);

    private final FluxSink<FrontendMessage> requests = this.requestProcessor.sink();

    private final Queue<MonoSink<Flux<BackendMessage>>> responseReceivers = Queues.<MonoSink<Flux<BackendMessage>>>unbounded().get();

    private final DirectProcessor<NotificationResponse> notificationProcessor = DirectProcessor.create();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private volatile Integer processId;

    private volatile Integer secretKey;

    private volatile TransactionStatus transactionStatus = IDLE;

    private volatile Version version = new Version("", 0);

    /**
     * Creates a new frame processor connected to a given TCP connection.
     *
     * @param connection the TCP connection
     * @throws IllegalArgumentException if {@code connection} is {@code null}
     */
    private ReactorNettyClient(Connection connection) {
        Assert.requireNonNull(connection, "Connection must not be null");

        connection.addHandler(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE - 5, 1, 4, -4, 0));
        connection.addHandler(new EnsureSubscribersCompleteChannelHandler(this.requestProcessor));
        this.connection = connection;
        this.byteBufAllocator = connection.outbound().alloc();

        AtomicReference<Throwable> receiveError = new AtomicReference<>();
        Mono<Void> receive = connection.inbound().receive()
            .map(BackendMessageDecoder::decode)
            .handle(this::handleResponse)
            .doOnError(throwable -> {
                receiveError.set(throwable);
                handleConnectionError(throwable);
            })
            .windowWhile(it -> it.getClass() != ReadyForQuery.class)
            .doOnNext(fluxOfMessages -> {
                MonoSink<Flux<BackendMessage>> receiver = this.responseReceivers.poll();
                if (receiver != null) {
                    receiver.success(fluxOfMessages.doOnComplete(() -> {

                        Throwable throwable = receiveError.get();
                        if (throwable != null) {
                            throw new PostgresConnectionException(throwable);
                        }

                        if (!isConnected()) {
                            throw EXPECTED.get();
                        }
                    }));
                }
            })
            .doOnComplete(this::handleClose)
            .then();

        Mono<Void> request = this.requestProcessor
            .flatMap(message -> {
                if (DEBUG_ENABLED) {
                    logger.debug("Request:  {}", message);
                }
                return connection.outbound().send(message.encode(this.byteBufAllocator));
            }, 1)
            .then();

        receive
            .onErrorResume(this::resumeError)
            .subscribe();

        request
            .onErrorResume(this::resumeError)
            .doAfterTerminate(this::handleClose)
            .subscribe();
    }

    private Mono<Void> resumeError(Throwable throwable) {

        handleConnectionError(throwable);
        this.requestProcessor.onComplete();

        if (isSslException(throwable)) {
            logger.debug("Connection Error", throwable);
        } else {
            logger.error("Connection Error", throwable);
        }

        return close();
    }

    private static boolean isSslException(Throwable throwable) {
        return throwable instanceof SSLException || throwable.getCause() instanceof SSLException;
    }

    private void handleResponse(BackendMessage message, SynchronousSink<BackendMessage> sink) {

        if (DEBUG_ENABLED) {
            logger.debug("Response: {}", message);
        }

        if (message.getClass() == NoticeResponse.class) {
            logger.warn("Notice: {}", toString(((NoticeResponse) message).getFields()));
            return;
        }

        if (message.getClass() == BackendKeyData.class) {

            BackendKeyData backendKeyData = (BackendKeyData) message;

            this.processId = backendKeyData.getProcessId();
            this.secretKey = backendKeyData.getSecretKey();
            return;
        }

        if (message.getClass() == ErrorResponse.class) {
            logger.warn("Error: {}", toString(((ErrorResponse) message).getFields()));
        }

        if (message.getClass() == ParameterStatus.class) {
            handleParameterStatus((ParameterStatus) message);
        }

        if (message.getClass() == ReadyForQuery.class) {
            this.transactionStatus = TransactionStatus.valueOf(((ReadyForQuery) message).getTransactionStatus());
        }

        if (message.getClass() == NotificationResponse.class) {
            this.notificationProcessor.onNext((NotificationResponse) message);
            return;
        }

        sink.next(message);
    }

    private void handleParameterStatus(ParameterStatus message) {

        Version existingVersion = this.version;

        String versionString = existingVersion.getVersion();
        int versionNum = existingVersion.getVersionNumber();

        if (message.getName().equals("server_version_num")) {
            versionNum = Integer.parseInt(message.getValue());
        }

        if (message.getName().equals("server_version")) {
            versionString = message.getValue();

            if (versionNum == 0) {
                versionNum = Version.parseServerVersionStr(versionString);
            }
        }

        this.version = new Version(versionString, versionNum);
    }

    /**
     * Creates a new frame processor connected to a given host.
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
     * Creates a new frame processor connected to a given host.
     *
     * @param host           the host to connect to
     * @param port           the port to connect to
     * @param connectTimeout connect timeout
     * @param sslConfig      SSL configuration
     * @throws IllegalArgumentException if {@code host} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(String host, int port, @Nullable Duration connectTimeout, SSLConfig sslConfig) {
        return connect(ConnectionProvider.newConnection(), InetSocketAddress.createUnresolved(host, port), connectTimeout, sslConfig);
    }

    /**
     * Creates a new frame processor connected to a given host.
     *
     * @param connectionProvider the connection provider resources
     * @param socketAddress      the socketAddress to connect to
     * @param connectTimeout     connect timeout
     * @param sslConfig          SSL configuration
     * @throws IllegalArgumentException if {@code host} is {@code null}
     */
    public static Mono<ReactorNettyClient> connect(ConnectionProvider connectionProvider, SocketAddress socketAddress, @Nullable Duration connectTimeout, SSLConfig sslConfig) {
        Assert.requireNonNull(connectionProvider, "connectionProvider must not be null");
        Assert.requireNonNull(socketAddress, "socketAddress must not be null");

        TcpClient tcpClient = TcpClient.create(connectionProvider).addressSupplier(() -> socketAddress);

        if (!(socketAddress instanceof InetSocketAddress)) {
            tcpClient = tcpClient.runOn(new SocketLoopResources(), true);
        }

        if (connectTimeout != null) {
            tcpClient = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(connectTimeout.toMillis()));
        }

        return tcpClient.connect().flatMap(it -> {

            ChannelPipeline pipeline = it.channel().pipeline();

            InternalLogger logger = InternalLoggerFactory.getInstance(ReactorNettyClient.class);
            if (logger.isTraceEnabled()) {
                pipeline.addFirst(LoggingHandler.class.getSimpleName(),
                    new LoggingHandler(ReactorNettyClient.class, LogLevel.TRACE));
            }

            return registerSslHandler(sslConfig, it).thenReturn(new ReactorNettyClient(it));
        });
    }

    private static Mono<? extends Void> registerSslHandler(SSLConfig sslConfig, Connection it) {

        if (sslConfig.getSslMode().startSsl()) {
            SSLSessionHandlerAdapter sslSessionHandlerAdapter = new SSLSessionHandlerAdapter(it.outbound().alloc(), sslConfig);
            it.addHandlerFirst(sslSessionHandlerAdapter);
            return sslSessionHandlerAdapter.getHandshake();
        }

        return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {

            drainError(EXPECTED);
            if (this.isClosed.compareAndSet(false, true)) {

                if (!isConnected() || this.processId == null) {
                    this.connection.dispose();
                    return this.connection.onDispose();
                }

                return Flux.just(Terminate.INSTANCE)
                    .doOnNext(message -> logger.debug("Request:  {}", message))
                    .concatMap(message -> this.connection.outbound().send(message.encode(this.connection.outbound().alloc())))
                    .then()
                    .doOnSuccess(v -> this.connection.dispose())
                    .then(this.connection.onDispose());
            }

            return Mono.empty();
        });
    }

    @Override
    public Flux<BackendMessage> exchange(Publisher<FrontendMessage> requests) {
        Assert.requireNonNull(requests, "requests must not be null");

        return Mono
            .<Flux<BackendMessage>>create(sink -> {

                final AtomicInteger once = new AtomicInteger();

                Flux.from(requests)
                    .subscribe(message -> {

                        if (!isConnected()) {
                            ReferenceCountUtil.safeRelease(message);
                            sink.error(new PostgresConnectionClosedException("Cannot exchange messages because the connection is closed"));
                            return;
                        }

                        if (once.get() == 0 && once.compareAndSet(0, 1)) {
                            synchronized (this) {
                                this.responseReceivers.add(sink);
                                this.requests.next(message);
                            }
                        } else {
                            this.requests.next(message);
                        }

                    }, this.requests::error, () -> {

                        if (!isConnected()) {
                            sink.error(new PostgresConnectionClosedException("Cannot exchange messages because the connection is closed"));
                        }
                    });

            })
            .flatMapMany(Function.identity());
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.byteBufAllocator;
    }

    @Override
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    @Override
    public Optional<Integer> getSecretKey() {
        return Optional.ofNullable(this.secretKey);
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

        if (this.requestProcessor.isDisposed()) {
            return false;
        }

        Channel channel = this.connection.channel();
        return channel.isOpen();
    }

    @Override
    public Disposable addNotificationListener(Consumer<NotificationResponse> consumer) {
        return this.notificationProcessor.subscribe(consumer);
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
        drainError(() -> new PostgresConnectionException(error));
    }

    private void drainError(Supplier<? extends Throwable> supplier) {
        MonoSink<Flux<BackendMessage>> receiver;

        while ((receiver = this.responseReceivers.poll()) != null) {
            receiver.error(supplier.get());
        }
    }

    private final class EnsureSubscribersCompleteChannelHandler extends ChannelDuplexHandler {

        private final EmitterProcessor<FrontendMessage> requestProcessor;

        private EnsureSubscribersCompleteChannelHandler(EmitterProcessor<FrontendMessage> requestProcessor) {
            this.requestProcessor = requestProcessor;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);

            this.requestProcessor.onComplete();
            handleClose();
        }
    }

    static class PostgresConnectionClosedException extends R2dbcNonTransientResourceException {

        public PostgresConnectionClosedException(String reason) {
            super(reason);
        }
    }

    static class PostgresConnectionException extends R2dbcNonTransientResourceException {

        public PostgresConnectionException(Throwable cause) {
            super(cause);
        }
    }

    static class SocketLoopResources implements LoopResources {

        @Nullable
        private static final Class<? extends Channel> EPOLL_SOCKET = findClass("io.netty.channel.epoll.EpollDomainSocketChannel");

        @Nullable
        private static final Class<? extends Channel> KQUEUE_SOCKET = findClass("io.netty.channel.kqueue.KQueueDomainSocketChannel");

        private static final boolean kqueue;

        static {
            boolean kqueueCheck = false;
            try {
                Class.forName("io.netty.channel.kqueue.KQueue");
                kqueueCheck = io.netty.channel.kqueue.KQueue.isAvailable();
            } catch (ClassNotFoundException cnfe) {
            }
            kqueue = kqueueCheck;
        }

        private static final boolean epoll;

        static {
            boolean epollCheck = false;
            try {
                Class.forName("io.netty.channel.epoll.Epoll");
                epollCheck = Epoll.isAvailable();
            } catch (ClassNotFoundException cnfe) {
            }
            epoll = epollCheck;
        }

        private final LoopResources delegate = TcpResources.get();

        @SuppressWarnings("unchecked")
        private static Class<? extends Channel> findClass(String className) {
            try {
                return (Class<? extends Channel>) SocketLoopResources.class.getClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }
        }

        @Override
        public Class<? extends Channel> onChannel(EventLoopGroup group) {

            if (epoll && EPOLL_SOCKET != null) {
                return EPOLL_SOCKET;
            }

            if (kqueue && KQUEUE_SOCKET != null) {
                return KQUEUE_SOCKET;
            }

            return this.delegate.onChannel(group);
        }

        @Override
        public EventLoopGroup onClient(boolean useNative) {
            return this.delegate.onClient(useNative);
        }

        @Override
        public Class<? extends DatagramChannel> onDatagramChannel(EventLoopGroup group) {
            return this.delegate.onDatagramChannel(group);
        }

        @Override
        public EventLoopGroup onServer(boolean useNative) {
            return this.delegate.onServer(useNative);
        }

        @Override
        public Class<? extends ServerChannel> onServerChannel(EventLoopGroup group) {
            return this.delegate.onServerChannel(group);
        }

        @Override
        public EventLoopGroup onServerSelect(boolean useNative) {
            return this.delegate.onServerSelect(useNative);
        }

        @Override
        public boolean preferNative() {
            return this.delegate.preferNative();
        }

        @Override
        public boolean daemon() {
            return this.delegate.daemon();
        }

        @Override
        public void dispose() {
            this.delegate.dispose();
        }

        @Override
        public Mono<Void> disposeLater() {
            return this.delegate.disposeLater();
        }
    }

}
