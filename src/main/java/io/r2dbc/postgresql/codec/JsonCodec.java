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

package io.r2dbc.postgresql.codec;

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

import java.io.*;
import java.util.Objects;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.*;

final class JsonCodec implements Codec<Object> {

    private static final Gson GSON = new Gson();
    private final ByteBufAllocator byteBufAllocator;

    JsonCodec(ByteBufAllocator byteBufAllocator) {
        this.byteBufAllocator = Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Objects.requireNonNull(format, "format must not be null");
        Objects.requireNonNull(type, "type must not be null");

        PostgresqlObjectId id = PostgresqlObjectId.valueOf(dataType);
        return FORMAT_TEXT == format && (JSON == id || JSONB == id);
    }

    @Override
    public final boolean canEncode(Object value) {
        return true;
    }

    @Override
    public final boolean canEncodeNull(Class<?> type) {
        return true;
    }

    @Nullable
    @Override
    public final Object decode(@Nullable ByteBuf byteBuf, Format format, Class<?> type) {
        if (byteBuf == null) {
            return null;
        }

        try {
            Reader reader = new InputStreamReader(new ByteBufInputStream(byteBuf));
            return GSON.fromJson(reader, type);
        } catch (Throwable t) {
            throw Exceptions.propagate(t);
        }
    }

    @Override
    public final Parameter encode(Object value) {
        Objects.requireNonNull(value, "value must not be null");

        try {
            ByteBuf encoded = byteBufAllocator.buffer();
            OutputStreamWriter writer = new OutputStreamWriter(new ByteBufOutputStream(encoded));

            GSON.toJson(value, writer);
            writer.close();

            return new Parameter(FORMAT_TEXT, JSON.getObjectId(), encoded);
        } catch (Throwable t) {
            throw Exceptions.propagate(t);
        }
    }

    @Override
    public Parameter encodeNull() {
        return new Parameter(FORMAT_TEXT, JSON.getObjectId(), null);
    }

}
