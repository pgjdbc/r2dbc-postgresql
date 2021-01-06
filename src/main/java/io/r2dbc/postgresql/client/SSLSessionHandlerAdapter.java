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

/**
 * SSL handler assuming the endpoint a Postgres endpoint following the {@link SSLRequest} flow.
 *
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11">https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11</a>
 */
final class SSLSessionHandlerAdapter extends AbstractPostgresSSLHandlerAdapter {

    private final ByteBufAllocator alloc;

    private final SSLConfig sslConfig;

    SSLSessionHandlerAdapter(ByteBufAllocator alloc, SSLConfig sslConfig) {
        super(alloc, sslConfig);
        this.alloc = alloc;
        this.sslConfig = sslConfig;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        Mono.from(SSLRequest.INSTANCE.encode(this.alloc)).subscribe(ctx::writeAndFlush);
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

    private void processSslDisabled(ChannelHandlerContext ctx, Object msg) {
        if (this.sslConfig.getSslMode().requireSsl()) {
            PostgresqlSslException e =
                new PostgresqlSslException("Server support for SSL connection is disabled, but client was configured with SSL mode " + this.sslConfig.getSslMode());
            completeHandshakeExceptionally(e);
        } else {
            completeHandshake();
        }
    }

    private void processSslEnabled(ChannelHandlerContext ctx, Object msg) {
        if (this.sslConfig.getSslMode() == SSLMode.DISABLE) {

            PostgresqlSslException e = new PostgresqlSslException("Server requires SSL handshake, but client was configured with SSL mode DISABLE");
            completeHandshakeExceptionally(e);
            return;
        }
        ctx.channel().pipeline()
            .addFirst(this.getSslHandler())
            .remove(this);
        ctx.fireChannelRead(msg);
    }

}
