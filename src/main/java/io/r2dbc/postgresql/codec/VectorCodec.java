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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Codec for pgvector.
 *
 * @since 1.0.3
 */
public class VectorCodec implements Codec<Vector>, CodecMetadata, ArrayCodecDelegate<Vector> {

    private final ByteBufAllocator byteBufAllocator;

    private final int oid;

    private final PostgresTypeIdentifier arrayOid;

    VectorCodec(ByteBufAllocator byteBufAllocator, int oid, int arrayOid) {
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.oid = oid;
        this.arrayOid = () -> arrayOid;
    }

    @Override
    public EncodedParameter encodeNull() {
        return new EncodedParameter(Format.FORMAT_BINARY, this.oid, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return Vector.class;
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return this.arrayOid;
    }

    @Override
    public Iterable<? extends PostgresTypeIdentifier> getDataTypes() {
        return Collections.singleton(AbstractCodec.getDataType(this.oid));
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return dataType == this.oid && (type == Object.class || Vector.class.isAssignableFrom(type) || type.isAssignableFrom(float[].class) || type.isAssignableFrom(Float[].class));
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Vector;
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        return (type == Object.class || Vector.class.isAssignableFrom(type) || type.isAssignableFrom(float[].class) || type.isAssignableFrom(Float[].class));
    }

    @Override
    public Vector decode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends Vector> type) {
        return decode(buffer, dataType.getObjectId(), format, type);
    }

    @Override
    public Vector decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends Vector> type) {
        return buffer == null ? null : Vector.of(decode(buffer, format));
    }

    static float[] decode(ByteBuf buffer, Format format) {

        if (format == Format.FORMAT_TEXT) {
            List<Number> objects = decodeText(buffer, (byte) ',');
            float[] values = new float[objects.size()];
            for (int i = 0; i < objects.size(); i++) {
                Number v = objects.get(i);
                values[i] = v.floatValue();
            }

            return values;
        }

        int dim = buffer.readShort();
        buffer.readShort(); // unused
        float[] values = new float[dim];
        for (int i = 0; i < dim; i++) {
            values[i] = buffer.readFloat();
        }

        return values;
    }

    @Override
    public EncodedParameter encode(Object value) {
        return encode(value, this.oid);
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {

        Assert.requireNonNull(value, "value must not be null");

        return new EncodedParameter(FORMAT_BINARY, dataType, Mono.fromSupplier(() -> encodeBinary(((Vector) value).getVector())));
    }

    @Override
    public String encodeToText(Vector value) {
        return value.toString();
    }

    private ByteBuf allocateBuffer(int dim) {
        return this.byteBufAllocator.buffer(estimateBufferSize(dim));
    }

    private ByteBuf encodeBinary(float[] values) {
        ByteBuf buffer = allocateBuffer(values.length);
        encodeBinary(buffer, values);
        return buffer;
    }

    static void encodeBinary(ByteBuf buffer, float[] values) {
        prepareBuffer(buffer, values.length);

        for (float v : values) {
            buffer.writeFloat(v);
        }
    }

    static int estimateBufferSize(int dim) {
        return 2 + 2 + (4 * dim);
    }

    private static void prepareBuffer(ByteBuf buffer, int dim) {
        buffer.writeShort(dim);
        buffer.writeShort(0); // unused
    }

    // duplicates ArrayCodec parsing.
    private static List<Number> decodeText(ByteBuf buf, byte delimiter) {

        boolean insideString = false;
        boolean wasInsideString = false; // needed for checking if NULL
        // value occurred
        List<Number> decoded = new ArrayList<>(); // array dimension arrays

        int indentEscape = 0;
        int readFrom = 0;
        boolean requiresEscapeCharFiltering = false;
        while (buf.isReadable()) {

            byte currentChar = buf.readByte();
            // escape character that we need to skip

            if (currentChar == '\\') {
                indentEscape++;
                buf.skipBytes(1);
                requiresEscapeCharFiltering = true;
            } else if (!insideString && currentChar == '[') {
                // subarray start

                for (int t = indentEscape + 1; t < buf.writerIndex(); t++) {
                    if (!Character.isWhitespace(buf.getByte(t)) && buf.getByte(t) != '[') {
                        break;
                    }
                }

                readFrom = buf.readerIndex();
            } else if (currentChar == '"') {
                // quoted element
                insideString = !insideString;
                wasInsideString = true;
            } else if (!insideString && Character.isWhitespace(currentChar)) {
                // white space
                continue;
            } else if ((!insideString && (currentChar == delimiter || currentChar == ']'))
                || indentEscape == buf.writerIndex() - 1) {
                // array end or element end
                // when character that is a part of array element
                int skipTrailingBytes = 0;
                if (currentChar != ']' && currentChar != delimiter && readFrom > 0) {
                    skipTrailingBytes++;
                }

                if (wasInsideString) {
                    readFrom++;
                    skipTrailingBytes++;
                }

                ByteBuf slice = buf.slice(readFrom, (buf.readerIndex() - readFrom) - (skipTrailingBytes + /* skip current char as we've over-read */ 1));
                try {
                    if (requiresEscapeCharFiltering) {
                        ByteBuf filtered = slice.alloc().buffer(slice.readableBytes());
                        while (slice.isReadable()) {
                            byte ch = slice.readByte();
                            if (ch == '\\') {
                                ch = slice.readByte();
                            }
                            filtered.writeByte(ch);
                        }
                        slice = filtered;
                    }

                    // add element to current array
                    if (slice.isReadable() || wasInsideString) {
                        if (!wasInsideString && slice.readableBytes() == 4 && slice.getByte(0) == 'N' && "NULL".equals(slice.toString(StandardCharsets.US_ASCII))) {
                            decoded.add(null);
                        } else {
                            decoded.add(NumericDecodeUtils.decodeNumber(slice, PostgresqlObjectId.FLOAT4, FORMAT_TEXT));
                        }
                    }
                } finally {

                    if (requiresEscapeCharFiltering) {
                        slice.release();
                    }
                }

                wasInsideString = false;
                requiresEscapeCharFiltering = false;
                readFrom = buf.readerIndex();
            }
        }

        return decoded;
    }

    /**
     * Array support for single-dimensional Vector arrays.
     */
    static class VectorArrayCodec extends ArrayCodec<Vector> {

        private final ByteBufAllocator byteBufAllocator;

        public VectorArrayCodec(ByteBufAllocator byteBufAllocator, VectorCodec delegate) {
            super(byteBufAllocator, delegate, Vector.class);
            this.byteBufAllocator = byteBufAllocator;
        }

        @Override
        public VectorCodec getDelegate() {
            return (VectorCodec) super.getDelegate();
        }

        @Override
        EncodedParameter doEncode(Object[] value, PostgresTypeIdentifier dataType) {
            boolean hasNulls = hasNulls(value);

            return new EncodedParameter(FORMAT_BINARY, dataType.getObjectId(), Mono.fromSupplier(() -> {

                ByteBuf buffer = this.byteBufAllocator.buffer();
                buffer.writeInt(1); // 1-dimensional vectors supported for now.

                buffer.writeInt(hasNulls ? 1 : 0); // flags: 0=no-nulls, 1=has-nulls
                buffer.writeInt(getDelegate().getArrayDataType().getObjectId());

                buffer.writeInt(value.length); // dimension size
                buffer.writeInt(0); // lower bound ignored

                for (Object o : value) {

                    if (o == null) {
                        buffer.writeInt(-1);
                    } else {
                        ByteBuf nested = this.byteBufAllocator.buffer();
                        VectorCodec.encodeBinary(nested, ((Vector) o).getVector());
                        buffer.writeInt(nested.readableBytes());
                        buffer.writeBytes(nested);
                        nested.release();
                    }
                }

                return buffer;
            }));
        }

        private static boolean hasNulls(Object[] value) {
            for (Object o : value) {
                if (o == null) {
                    return true;
                }
            }
            return false;
        }
    }

}
