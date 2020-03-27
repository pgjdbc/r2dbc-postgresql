/*
 * Copyright 2017-2020 the original author or authors.
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
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;

@SuppressWarnings("rawtypes")
final class HStoreCodec implements Codec<Map> {

    private final ByteBufAllocator byteBufAllocator;
    private final Class<Map> type = Map.class;
    private final int oid;

    /**
     * Creates a new {@link HStoreCodec}.
     *
     * @param byteBufAllocator the type handled by this codec
     */
    HStoreCodec(ByteBufAllocator byteBufAllocator, int oid) {
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.oid = oid;
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return dataType == oid && isTypeAssignable(type);
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.type.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return this.type.isAssignableFrom(type);
    }

    @Override
    public Map<String, String> decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends Map> type) {
        if (buffer == null) {
            return null;
        } else if (format == Format.FORMAT_TEXT) {
            return doTextDecode(buffer);
        }
        return doBinaryDecode(buffer);
    }

    private Map<String, String> doBinaryDecode(ByteBuf buffer) {
        Map<String, String> m = new HashMap<>();
        int pos = 0;
        int numElements = buffer.getInt(pos);
        pos += 4;

        for (int i = 0; i < numElements; ++i) {
            int keyLen = buffer.getInt(pos);
            pos += 4;
            String key = buffer.getCharSequence(pos, keyLen, Charset.defaultCharset()).toString();
            pos += keyLen;
            int valLen = buffer.getInt(pos);
            pos += 4;
            String val;
            if (valLen == -1) {
                val = null;
            } else {
                val = buffer.getCharSequence(pos, valLen, Charset.defaultCharset()).toString();
                pos += valLen;
            }
            m.put(key, val);
        }
        return m;
    }

    private Map<String, String> doTextDecode(ByteBuf buffer) {
        Map<String, String> map = new HashMap<>();
        int pos = 0;
        int size = buffer.readableBytes();
        StringBuilder sb = new StringBuilder();

        while (pos < size) {
            pos = setString(sb, buffer, pos, size) + 3; // append '=>'
            String key = sb.toString();
            String value;

            if ((char) buffer.getByte(pos) == 'N') {
                value = null;
                pos += 4; // append 'NULL'
            } else {
                pos = setString(sb, buffer, pos, size) + 1;
                value = sb.toString();
            }
            map.put(key, value);
        }
        return map;
    }

    private int setString(StringBuilder sb, ByteBuf buffer, int pos, int size) {
        sb.setLength(0);
        while (pos < size && buffer.getByte(pos) != '"') {
            pos++;
        }
        for (pos += 1; pos < size; pos++) {
            char c = (char) buffer.getByte(pos);
            if (c == '"') {
                break;
            } else if (c == '\\') {
                pos++;
                c = (char) buffer.getByte(pos);
            }
            sb.append(c);
        }
        return pos;
    }

    @Override
    public Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");
        Map<?, ?> map = (Map<?, ?>) value;

        return new Parameter(Format.FORMAT_BINARY, oid, Mono.fromSupplier(() -> {
            ByteBuf buffer = byteBufAllocator.buffer(4 + 10 * map.size());
            buffer.writeInt(map.size());

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String k = entry.getKey().toString();
                buffer.writeInt(k.length());
                buffer.writeCharSequence(k, Charset.defaultCharset());

                if (entry.getValue() == null) {
                    buffer.writeInt(-1);
                } else {
                    String v = entry.getValue().toString();
                    buffer.writeInt(v.length());
                    buffer.writeCharSequence(v, Charset.defaultCharset());
                }
            }
            return buffer;
        }));
    }

    @Override
    public Parameter encodeNull() {
        return new Parameter(Format.FORMAT_BINARY, oid, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return type;
    }

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return type.isAssignableFrom(this.type);
    }
}
