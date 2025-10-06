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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

@SuppressWarnings("rawtypes")
final class HStoreCodec implements Codec<Map>, CodecMetadata {

    /**
     * Aborts on a comma {@code ('"')}.
     */
    static final ByteProcessor FIND_QUOTE = new ByteProcessor.IndexOfProcessor((byte) '"');

    private static final Class<Map> TYPE = Map.class;

    private final ByteBufAllocator byteBufAllocator;

    private final int oid;

    /**
     * Create a new {@link HStoreCodec}.
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

        return dataType == this.oid && type.isAssignableFrom(TYPE);
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return TYPE.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return TYPE.isAssignableFrom(type);
    }

    @Override
    public Map<String, String> decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends Map> type) {
        if (buffer == null) {
            return null;
        }

        if (format == FORMAT_TEXT) {
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

        while (buffer.isReadable()) {
            String key = readString(buffer);
            if (key == null) {
                break;
            }
            buffer.skipBytes(2); // skip '=>'
            String value;

            if ((char) peekByte(buffer) == 'N') {
                value = null;
                buffer.skipBytes(4);// skip 'NULL'.
            } else {
                value = readString(buffer);
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

    private static @Nullable String readString(ByteBuf buffer) {

        int position = buffer.forEachByte(FIND_QUOTE);
        if (position > buffer.writerIndex()) {
            return null;
        }

        if (position > -1) {
            buffer.readerIndex(position + 1);
        }

        ByteBuf aggregated = buffer.alloc().buffer();
        try {
            while (buffer.isReadable()) {
                byte current = buffer.readByte();

                if (current == '"') {
                    break;
                }

                aggregated.writeByte(current == '\\' ? buffer.readByte() : current);
            }

            return aggregated.toString(StandardCharsets.UTF_8);
        } finally {
            aggregated.release();
        }
    }

    @Override
    public EncodedParameter encode(Object value) {
        return encode(value, this.oid);
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {
        Assert.requireNonNull(value, "value must not be null");
        Map<?, ?> map = (Map<?, ?>) value;

        return new EncodedParameter(FORMAT_BINARY, dataType, Mono.fromSupplier(() -> {
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
    public EncodedParameter encodeNull() {
        return encodeNull(this.oid);
    }

    private EncodedParameter encodeNull(int dataType) {
        return new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return TYPE;
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return Collections.singleton(AbstractCodec.getDataType(this.oid));
    }

}
