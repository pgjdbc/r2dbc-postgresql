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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;

abstract class AbstractCodec<T> implements Codec<T> {

    private final Class<T> type;

    AbstractCodec(Class<T> type) {
        this.type = Assert.requireNonNull(type, "type must not be null");
    }

    @Override
    public final boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return (type == Object.class || isTypeAssignable(type)) &&
            doCanDecode(format, PostgresqlObjectId.valueOf(dataType));
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.type.isInstance(value);
    }

    @Override
    public final boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return this.type.isAssignableFrom(type);
    }

    @Nullable
    @Override
    public final T decode(@Nullable ByteBuf byteBuf, Format format, Class<? extends T> type) {
        if (byteBuf == null) {
            return null;
        }

        return doDecode(byteBuf, format, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return doEncode((T) value);
    }

    @Override
    public Class<?> type() {
        return this.type;
    }

    static Parameter create(Format format, PostgresqlObjectId type, @Nullable Publisher<? extends ByteBuf> value) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return new Parameter(format, type.getObjectId(), value);
    }

    static Parameter createNull(Format format, PostgresqlObjectId type) {
        return create(format, type, NULL_VALUE);
    }

    abstract boolean doCanDecode(Format format, PostgresqlObjectId type);

    abstract T doDecode(ByteBuf byteBuf, Format format, Class<? extends T> type);

    abstract Parameter doEncode(T value);

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return type.isAssignableFrom(this.type);
    }

}
