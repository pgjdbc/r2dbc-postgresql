/*
 * Copyright 2020 the original author or authors.
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
import io.netty.util.ByteProcessor;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;

@SuppressWarnings("rawtypes")
final class HStoreCodec implements Codec<Map> {

    /**
     * A {@link ByteProcessor} which finds the first appearance of a specific byte.
     */
    static class IndexOfProcessor implements ByteProcessor {

        private final byte byteToFind;

        public IndexOfProcessor(byte byteToFind) {
            this.byteToFind = byteToFind;
        }

        @Override
        public boolean process(byte value) {
            return value != this.byteToFind;
        }
    }

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

        Assert.requireNonNull(type, "type must not be null");

        return dataType == this.oid && type.isAssignableFrom(this.type);
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
        }

        if (format == Format.FORMAT_TEXT) {
            return decodeTextFormat(buffer);
        }

        return decodeBinaryFormat(buffer);
    }

    private static Map<String, String> decodeBinaryFormat(ByteBuf buffer) {
        Map<String, String> map = new LinkedHashMap<>();
        if (!buffer.isReadable()) {
            return map;
        }

        int numElements = buffer.readInt();

        for (int i = 0; i < numElements; ++i) {
            int keyLen = buffer.readInt();
            String key = buffer.readCharSequence(keyLen, UTF_8).toString();
            int valLen = buffer.readInt();
            String val = valLen == -1 ? null : buffer.readCharSequence(valLen, UTF_8).toString();

            map.put(key, val);
        }
        return map;
    }

    private static Map<String, String> decodeTextFormat(ByteBuf buffer) {
        Map<String, String> map = new LinkedHashMap<>();
        StringBuilder mutableBuffer = new StringBuilder();

        while (buffer.isReadable()) {
            String key = readString(mutableBuffer, buffer);
            if (key == null) {
                break;
            }
            buffer.skipBytes(2); // skip '=>'
            String value;

            if ((char) peekByte(buffer) == 'N') {
                value = null;
                buffer.skipBytes(4);// skip 'NULL'.
            } else {
                value = readString(mutableBuffer, buffer);
            }
            map.put(key, value);

            if (buffer.isReadable()) {
                buffer.readByte(); // skip ','
            }

        }
        return map;
    }

    private static byte peekByte(ByteBuf buffer) {

        int readerIndex = buffer.readerIndex();
        try {
            return buffer.readByte();
        } finally {
            buffer.readerIndex(readerIndex);
        }
    }

    @Nullable
    private static String readString(StringBuilder mutableBuffer, ByteBuf buffer) {
        mutableBuffer.setLength(0);
        int position = buffer.forEachByte(new IndexOfProcessor((byte) '"'));
        if (position > buffer.writerIndex()) {
            return null;
        }

        if (position > -1) {
            buffer.readerIndex(position + 1);
        }

        while (buffer.isReadable()) {
            char c = (char) buffer.readByte();
            if (c == '"') {
                break;
            } else if (c == '\\') {
                c = (char) buffer.readByte();
            }
            mutableBuffer.append(c);
        }

        String result = mutableBuffer.toString();
        mutableBuffer.setLength(0);
        return result;
    }

    @Override
    public Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");
        Map<?, ?> map = (Map<?, ?>) value;

        return new Parameter(Format.FORMAT_BINARY, this.oid, Mono.fromSupplier(() -> {
            ByteBuf buffer = this.byteBufAllocator.buffer(4 + 10 * map.size());
            buffer.writeInt(map.size());

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String k = entry.getKey().toString();
                byte[] bytes = k.getBytes(UTF_8);
                buffer.writeInt(bytes.length);
                buffer.writeBytes(bytes);

                if (entry.getValue() == null) {
                    buffer.writeInt(-1);
                } else {
                    String v = entry.getValue().toString();
                    bytes = v.getBytes(UTF_8);
                    buffer.writeInt(bytes.length);
                    buffer.writeBytes(bytes);
                }
            }
            return buffer;
        }));
    }

    @Override
    public Parameter encodeNull() {
        return new Parameter(Format.FORMAT_BINARY, this.oid, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return this.type;
    }

}
