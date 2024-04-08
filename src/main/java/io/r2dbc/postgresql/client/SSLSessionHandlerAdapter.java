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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.r2dbc.postgresql.message.frontend.SSLRequest;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;

/**
 * SSL handler assuming the endpoint a Postgres endpoint following the {@link SSLRequest} flow.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11">https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11</a>
 */
final class SSLSessionHandlerAdapter extends AbstractPostgresSSLHandlerAdapter {

    private final ByteBufAllocator alloc;

    private final SSLConfig sslConfig;

    private boolean negotiating = true;

    SSLSessionHandlerAdapter(ByteBufAllocator alloc, SocketAddress socketAddress, SSLConfig sslConfig) {
        super(alloc, socketAddress, sslConfig);
        this.alloc = alloc;
        this.sslConfig = sslConfig;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (this.negotiating) {
            Mono.from(SSLRequest.INSTANCE.encode(this.alloc)).subscribe(ctx::writeAndFlush);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.negotiating) {
            // If we receive channel inactive before negotiated, then the inbound has closed early.
            PostgresqlSslException e = new PostgresqlSslException("Connection closed during SSL negotiation");
            completeHandshakeExceptionally(e);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (this.negotiating) {
            ByteBuf buf = (ByteBuf) msg;
            char response = (char) buf.readByte();
            try {
                switch (response) {
                    case 'S':
                        processSslEnabled(ctx, buf);
                        break;
                    case 'N':
                        processSslDisabled();
                        break;
                    default:
                        throw new IllegalStateException("Unknown SSLResponse from server: '" + response + "'");
                }
            } finally {
                buf.release();
                this.negotiating = false;
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    private void processSslDisabled() {
        if (this.sslConfig.getSslMode().requireSsl()) {
            PostgresqlSslException e =
                new PostgresqlSslException("Server support for SSL connection is disabled, but client was configured with SSL mode " + this.sslConfig.getSslMode());
            completeHandshakeExceptionally(e);
        } else {
            completeHandshake();
        }
    }

    private void processSslEnabled(ChannelHandlerContext ctx, ByteBuf msg) {
        if (this.sslConfig.getSslMode() == SSLMode.DISABLE) {

            PostgresqlSslException e = new PostgresqlSslException("Server requires SSL handshake, but client was configured with SSL mode DISABLE");
            completeHandshakeExceptionally(e);
            return;
        }
        ctx.channel().pipeline().addFirst(this.getSslHandler());
        ctx.fireChannelRead(msg.retain());
    }

}
