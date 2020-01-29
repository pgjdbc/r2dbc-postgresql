/*
 * Copyright 2019-2020 the original author or authors.
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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CopyData;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.CopyDone;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.replication.LogSequenceNumber;
import io.r2dbc.postgresql.replication.ReplicationRequest;
import io.r2dbc.postgresql.replication.ReplicationStream;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

final class PostgresReplicationStream implements ReplicationStream {

    public static final long POSTGRES_EPOCH_2000_01_01 = 946684800000L;

    private static final char KEEP_ALIVE = 'k';

    private static final char X_LOG_DATA = 'w';

    private final EmitterProcessor<CopyData> responseProcessor = EmitterProcessor.create(false);

    private final EmitterProcessor<FrontendMessage> requestProcessor;

    private final AtomicReference<Disposable> subscription = new AtomicReference<>();

    private final ByteBufAllocator allocator;

    private final ReplicationRequest replicationRequest;

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private volatile LogSequenceNumber lastServerLSN = LogSequenceNumber.INVALID_LSN;

    private volatile LogSequenceNumber lastReceiveLSN = LogSequenceNumber.INVALID_LSN;

    private volatile LogSequenceNumber lastAppliedLSN = LogSequenceNumber.INVALID_LSN;

    private volatile LogSequenceNumber lastFlushedLSN = LogSequenceNumber.INVALID_LSN;

    PostgresReplicationStream(ByteBufAllocator allocator, ReplicationRequest replicationRequest, EmitterProcessor<FrontendMessage> requestProcessor, Flux<BackendMessage> messages) {
        this.allocator = allocator;
        this.replicationRequest = replicationRequest;
        this.requestProcessor = requestProcessor;

        Flux<CopyData> stream = messages
            .takeUntil(ReadyForQuery.class::isInstance)
            .doOnError(throwable -> {
                close().subscribe();
                this.closeFuture.complete(null);
            })
            .doOnComplete(() -> {
                this.closeFuture.complete(null);
            })
            .ofType(CopyData.class)
            .handle((message, sink) -> {

                try {

                    byte code = message.getData().readByte();

                    switch (code) {

                        case KEEP_ALIVE: //KeepAlive message
                            if (processKeepAliveMessage(message.getData()) || this.replicationRequest.getStatusInterval().isZero()) {
                                sendStatusUpdate();
                            }
                            return;

                        case X_LOG_DATA: //XLogData
                            processXLogData(message.getData());
                            message.retain();
                            sink.next(message);
                            return;

                        default:
                            sink.error(new R2dbcNonTransientResourceException(String.format("Unexpected packet type during replication: %s", Integer.toString(code))));
                    }
                } finally {
                    ReferenceCountUtil.release(message);
                }
            });

        stream.subscribeWith(this.responseProcessor);
        Disposable disposable = () -> {
        };

        Duration statusInterval = replicationRequest.getStatusInterval();
        if (!statusInterval.isZero()) {

            Scheduler.Worker worker = Schedulers.parallel().createWorker();

            worker.schedulePeriodically(this::sendStatusUpdate, statusInterval.toMillis(), statusInterval.toMillis(), TimeUnit.MILLISECONDS);

            disposable = worker;
        }

        this.subscription.set(disposable);
    }

    private boolean processKeepAliveMessage(ByteBuf buffer) {

        this.lastServerLSN = LogSequenceNumber.valueOf(buffer.readLong());
        if (this.lastServerLSN.asLong() > this.lastReceiveLSN.asLong()) {
            this.lastReceiveLSN = this.lastServerLSN;
        }

        long lastServerClock = buffer.readLong();
        boolean replyRequired = buffer.readByte() != 0;

        return replyRequired;
    }

    private void processXLogData(ByteBuf buffer) {

        long startLsn = buffer.readLong();
        this.lastServerLSN = LogSequenceNumber.valueOf(buffer.readLong());
        long systemClock = buffer.readLong();

        switch (this.replicationRequest.getReplicationType()) {
            case LOGICAL:
                this.lastReceiveLSN = LogSequenceNumber.valueOf(startLsn);
                break;
            case PHYSICAL:
                int payloadSize = buffer.readableBytes() - buffer.readerIndex();
                this.lastReceiveLSN = LogSequenceNumber.valueOf(startLsn + payloadSize);
                break;
        }
    }

    private void sendStatusUpdate() {
        ByteBuf byteBuf = prepareUpdateStatus(this.lastReceiveLSN, this.lastFlushedLSN, this.lastAppliedLSN, false);
        io.r2dbc.postgresql.message.frontend.CopyData copyData = new io.r2dbc.postgresql.message.frontend.CopyData(byteBuf);
        this.requestProcessor.onNext(copyData);
    }

    private ByteBuf prepareUpdateStatus(LogSequenceNumber received, LogSequenceNumber flushed,
                                        LogSequenceNumber applied, boolean replyRequired) {

        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        // consider range bounds
        long systemClock = TimeUnit.MICROSECONDS.convert((now - POSTGRES_EPOCH_2000_01_01), TimeUnit.MICROSECONDS);

        return new KeepAliveMessage(received, flushed, applied, systemClock, replyRequired).encode(this.allocator);
    }

    @Override
    public Mono<Void> close() {

        Disposable disposable = this.subscription.get();
        if (disposable != null && this.subscription.compareAndSet(disposable, null)) {
            disposable.dispose();

            this.requestProcessor.onNext(CopyDone.INSTANCE);
            this.requestProcessor.onComplete();

            return this.responseProcessor.ignoreElements().then(Mono.fromCompletionStage(this.closeFuture));
        }

        return Mono.fromCompletionStage(this.closeFuture);
    }

    @Override
    public boolean isClosed() {
        return this.subscription.get() == null;
    }

    @Override
    public <T> Flux<T> map(Function<ByteBuf, ? extends T> mappingFunction) {
        Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
        return this.responseProcessor.map(data -> {

            try {
                return mappingFunction.apply(data.getData());
            } finally {
                ReferenceCountUtil.release(data);
            }
        });
    }

    @Override
    public LogSequenceNumber getLastReceiveLSN() {
        return this.lastReceiveLSN;
    }

    @Override
    public LogSequenceNumber getLastFlushedLSN() {
        return this.lastFlushedLSN;
    }

    @Override
    public LogSequenceNumber getLastAppliedLSN() {
        return this.lastAppliedLSN;
    }

    @Override
    public void setFlushedLSN(LogSequenceNumber flushed) {
        this.lastFlushedLSN = flushed;
    }

    @Override
    public void setAppliedLSN(LogSequenceNumber applied) {
        this.lastAppliedLSN = applied;
    }

}
