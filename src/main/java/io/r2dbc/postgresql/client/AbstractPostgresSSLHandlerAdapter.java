/*
 * Copyright 2017 the original author or authors.
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

abstract class AbstractPostgresSSLHandlerAdapter extends ChannelInboundHandlerAdapter implements GenericFutureListener<Future<Channel>> {

    private final SSLConfig sslConfig;

    private final SSLEngine sslEngine;

    private final SslHandler sslHandler;

    private final CompletableFuture<Void> handshakeFuture;

    AbstractPostgresSSLHandlerAdapter(ByteBufAllocator alloc, SSLConfig sslConfig) {
        this.sslConfig = sslConfig;
        this.sslEngine = sslConfig.getSslProvider().get().newEngine(alloc);
        this.handshakeFuture = new CompletableFuture<>();
        this.sslHandler = new SslHandler(this.sslEngine);
        this.sslHandler.handshakeFuture().addListener(this);
    }

    @Override
    public void operationComplete(Future<Channel> future) throws Exception {
        if (!future.isSuccess()) {
            completeHandshakeExceptionally(future.cause());
            return;
        }
        if (this.sslConfig.getSslMode() != SSLMode.VERIFY_FULL) {
            completeHandshake();
            return;
        }
        Channel channel = future.get();
        InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
        String hostName = socketAddress.getHostName();
        if (this.sslConfig.getHostnameVerifier().verify(hostName, this.sslEngine.getSession())) {
            completeHandshake();
        } else {
            completeHandshakeExceptionally(new PostgresqlSslException(String.format("The hostname '%s' could not be verified.", socketAddress.getAddress().toString())));
        }
    }

    void completeHandshakeExceptionally(Throwable ex) {
        this.handshakeFuture.completeExceptionally(ex);
    }

    boolean completeHandshake() {
        return this.handshakeFuture.complete(null);
    }

    Mono<Void> getHandshake() {
        return Mono.fromFuture(this.handshakeFuture);
    }

    SslHandler getSslHandler() {
        return this.sslHandler;
    }

    /**
     * Postgres-specific {@link R2dbcPermissionDeniedException}.
     */
    static final class PostgresqlSslException extends R2dbcPermissionDeniedException {

        PostgresqlSslException(String msg) {
            super(msg);
        }

    }

}
