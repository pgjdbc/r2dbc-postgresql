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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Arrays;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;

final class ClobCodec extends AbstractCodec<Clob> {

    private final ByteBufAllocator byteBufAllocator;

    ClobCodec(ByteBufAllocator byteBufAllocator) {
        super(Clob.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(TEXT, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return TEXT == type;
    }

    @Override
    Clob doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends Clob> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return Clob.from(Mono.just(ByteBufUtils.decode(buffer)));
    }

    @Override
    Parameter doEncode(Clob value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(VARCHAR, FORMAT_TEXT,
            Flux.from(value.stream())
                .reduce(new StringBuilder(), StringBuilder::append)
                .map(sb -> ByteBufUtils.encode(this.byteBufAllocator, sb.toString()))
                .concatWith(Flux.from(value.discard())
                    .then(Mono.empty()))
        );
    }

    @Override
    public Iterable<PostgresqlObjectId> getDataTypes() {
        return Arrays.asList(VARCHAR, TEXT);
    }

    @Override
    public Iterable<Format> getFormats() {
        return Arrays.asList(FORMAT_TEXT, FORMAT_BINARY);
    }

}
