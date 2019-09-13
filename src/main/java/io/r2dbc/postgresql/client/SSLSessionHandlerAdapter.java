/*
 * Copyright 2017-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.r2dbc.postgresql.message.frontend.SSLRequest;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11
public class SSLSessionHandlerAdapter extends ChannelInboundHandlerAdapter implements GenericFutureListener<Future<Channel>> {

    private final ByteBufAllocator alloc;

    private final SSLConfig sslConfig;

    private final SSLEngine sslEngine;

    private final SslHandler sslHandler;

    private final CompletableFuture<Void> handshakeFuture;

    public SSLSessionHandlerAdapter(ByteBufAllocator alloc, SSLConfig sslConfig) {
        this.alloc = alloc;
        this.sslConfig = sslConfig;
        this.sslEngine = sslConfig.getSslProvider()
            .getSslContext()
            .newEngine(alloc);
        this.handshakeFuture = new CompletableFuture<>();
        this.sslHandler = new SslHandler(sslEngine);
        this.sslHandler.handshakeFuture().addListener(this);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        char response = (char) buf.readByte();
        switch (response) {
            case 'S':
                processSslEnabled(ctx, msg);
                break;
            case 'N':
                processSslDisabled(ctx, msg);
                break;
            default:
                throw new IllegalStateException("Unknown SSLResponse from server: '" + response + "'");
        }
    }

    @Override
    public void operationComplete(Future<Channel> future) throws Exception {
        if (!future.isSuccess()) {
            this.handshakeFuture.completeExceptionally(future.cause());
            return;
        }
        if (this.sslConfig.getSslMode() != SSLMode.VERIFY_FULL) {
            this.handshakeFuture.complete(null);
            return;
        }
        Channel channel = future.get();
        InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
        String hostName = socketAddress.getHostName();
        if (this.sslConfig.getHostnameVerifier().verify(hostName, this.sslEngine.getSession())) {
            this.handshakeFuture.complete(null);
        } else {
            this.handshakeFuture.completeExceptionally(new IllegalStateException(String.format("The hostname '%s' could not be verified by hostnameverifier.", socketAddress.getAddress().toString())));
        }
    }

    private void processSslDisabled(ChannelHandlerContext ctx, Object msg) {
        if (this.sslConfig.getSslMode().requireSsl()) {
            throw new IllegalStateException("Server support for SSL connection is disabled, but client was configured with SSL mode " + this.sslConfig.getSslMode());
        } else {
            handshakeFuture.complete(null);
        }
    }

    private void processSslEnabled(ChannelHandlerContext ctx, Object msg) {
        if (this.sslConfig.getSslMode() == SSLMode.DISABLE) {
            throw new IllegalStateException("Server requires SSL handshake, but client was configured with SSL mode DISABLE");
        }
        ctx.channel().pipeline()
            .addFirst(this.sslHandler)
            .remove(this);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        Mono.from(SSLRequest.INSTANCE.encode(this.alloc)).subscribe(ctx::writeAndFlush);
    }

    public Mono<Void> handshakeFuture() {
        return Mono.fromFuture(this.handshakeFuture);
    }
}
