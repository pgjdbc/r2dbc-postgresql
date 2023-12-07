/*
 * Copyright 2023 the original author or authors.
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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Collections;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

public class VectorFloatCodec implements Codec<float[]>, CodecMetadata {

    private final ByteBufAllocator byteBufAllocator;

    private final int oid;

    VectorFloatCodec(ByteBufAllocator byteBufAllocator, int oid) {
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.oid = oid;
    }

    @Override
    public EncodedParameter encodeNull() {
        return new EncodedParameter(Format.FORMAT_BINARY, this.oid, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return float[].class;
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return Collections.singleton(AbstractCodec.getDataType(this.oid));
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return dataType == this.oid && type == float[].class;
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof float[];
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        return false;
    }

    @Override
    public float[] decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends float[]> type) {

        if (buffer == null) {
            return null;
        }

        return VectorCodec.decode(buffer, format);
    }

    @Override
    public EncodedParameter encode(Object value) {
        return encode(value, this.oid);
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {

        Assert.requireNonNull(value, "value must not be null");

        float[] vec = (float[]) value;
        return new EncodedParameter(FORMAT_BINARY, dataType, Mono.fromSupplier(() -> {

            ByteBuf buffer = this.byteBufAllocator.buffer(VectorCodec.estimateBufferSize(vec.length));
            VectorCodec.encodeBinary(buffer, vec);
            return buffer;
        }));
    }

}
