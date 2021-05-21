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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;

import java.util.Collections;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8_ARRAY;

/**
 * @since 0.8.8
 */
final class DoubleArrayCodec extends AbstractArrayCodec<Double> {

    DoubleArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, Double.class);
    }

    @Override
    public Parameter encodeNull() {
        return createNull(FLOAT8_ARRAY, FORMAT_TEXT);
    }

    @Override
    public Iterable<PostgresqlObjectId> getDataTypes() {
        return Collections.singleton(FLOAT8_ARRAY);
    }

    @Override
    Double doDecodeBinary(ByteBuf byteBuffer) {
        return byteBuffer.readDouble();
    }

    @Override
    Double doDecodeText(String text) {
        return Double.parseDouble(text);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return FLOAT8_ARRAY == type;
    }

    @Override
    Parameter encodeArray(Supplier<ByteBuf> encodedSupplier) {
        return create(FLOAT8_ARRAY, FORMAT_TEXT, encodedSupplier);
    }

    @Override
    String doEncodeText(Double value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.toString();
    }

}
