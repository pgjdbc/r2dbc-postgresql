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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Generic codec class that provides a basis for implementations
 * of a array-typed {@link Codec}. It uses a {@link ArrayCodecDelegate delegate}
 * that can encode/decode the underlying type of the array.
 *
 * @param <T> the type that is handled by this {@link Codec}
 */
class ArrayCodec<T> extends AbstractCodec<Object[]> {

    static final byte CLOSE_CURLY = '}';

    static final byte COMMA = ',';

    static final String NULL = "NULL";

    static final byte OPEN_CURLY = '{';

    private final ArrayCodecDelegate<T> delegate;

    private final ByteBufAllocator byteBufAllocator;

    private final Class<T> componentType;

    private final PostgresTypeIdentifier dataType;

    private final byte delimiter;

    /**
     * Create a new {@link ArrayCodec}.
     *
     * @param byteBufAllocator the buffer allocator
     * @param delegate         the underlying {@link ArrayCodecDelegate} used to encode/decode data
     * @param componentType    the target component type
     */
    ArrayCodec(ByteBufAllocator byteBufAllocator, ArrayCodecDelegate<T> delegate, Class<T> componentType) {
        super(Object[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.delegate = Assert.requireNonNull(delegate, "delegate must not be null");
        this.dataType = delegate.getArrayDataType();
        this.componentType = Assert.requireNonNull(componentType, "componentType must not be null");
        this.delimiter = COMMA;
    }

    /**
     * Create a new {@link ArrayCodec}.
     *
     * @param byteBufAllocator the buffer allocator
     * @param dataType         the Postgres OID handled by this codec
     * @param delegate         the underlying {@link ArrayCodecDelegate} used to encode/decode data
     * @param componentType    the target component type
     */
    ArrayCodec(ByteBufAllocator byteBufAllocator, PostgresTypeIdentifier dataType, ArrayCodecDelegate<T> delegate, Class<T> componentType) {
        super(Object[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.dataType = Assert.requireNonNull(dataType, "dataType must not be null");
        this.delegate = Assert.requireNonNull(delegate, "delegate must not be null");
        this.componentType = Assert.requireNonNull(componentType, "componentType must not be null");
        this.delimiter = COMMA;
    }

    /**
     * Create a new {@link ArrayCodec}.
     *
     * @param byteBufAllocator the buffer allocator
     * @param dataType         the Postgres OID handled by this codec
     * @param delegate         the underlying {@link ArrayCodecDelegate} used to encode/decode data
     * @param componentType    the target component type
     * @param delimiter        the delimiter to use when encoding
     */
    ArrayCodec(ByteBufAllocator byteBufAllocator, PostgresTypeIdentifier dataType, ArrayCodecDelegate<T> delegate, Class<T> componentType, byte delimiter) {
        super(Object[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.dataType = Assert.requireNonNull(dataType, "dataType must not be null");
        this.delegate = Assert.requireNonNull(delegate, "delegate must not be null");
        this.componentType = Assert.requireNonNull(componentType, "componentType must not be null");
        this.delimiter = delimiter;
    }

    public ArrayCodecDelegate<T> getDelegate() {
        return this.delegate;
    }

    public Class<T> getComponentType() {
        return this.componentType;
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return isTypeAssignable(value.getClass());
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return isTypeAssignable(type);
    }

    @Override
    public EncodedParameter encodeNull() {
        return encodeNull(this.dataType.getObjectId());
    }

    static String escapeArrayElement(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            if (c == '"' || c == '\\') {
                b.append('\\');
            }
            b.append(c);
        }
        b.append('"');
        return b.toString();
    }

    @Override
    Object[] doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends Object[]> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (FORMAT_BINARY == format) {
            return decodeBinary(buffer, this.dataType, this.delegate, this.componentType, type);
        } else {
            return decodeText(buffer, this.dataType, this.delimiter, this.delegate, this.componentType, type);
        }
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        return this.dataType.getObjectId() == dataType && (type == Object.class || (type.isArray() && getActualComponentType(type).isAssignableFrom(this.componentType)));
    }

    Class<?> getActualComponentType(Class<?> requestedType) {

        Class<?> type = requestedType;
        while (type.isArray()) {
            type = type.getComponentType();
        }

        return type;
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(type, "type must not be null");

        return this.dataType.equals(type);
    }

    @Override
    final EncodedParameter doEncode(Object[] value) {
        return doEncode(value, this.dataType);
    }

    @Override
    EncodedParameter doEncode(Object[] value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return encodeArray(() -> {
            ByteBuf byteBuf = this.byteBufAllocator.buffer();
            encodeAsText(byteBuf, value, this.delegate::encodeToText);
            return byteBuf;
        }, dataType);
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return Collections.singleton(this.dataType);
    }

    /**
     * Create the encoded array representation.
     *
     * @param encodedSupplier supplies the encoded buffers
     * @param dataType        the Postgres data type
     * @return encoded {@link EncodedParameter} item
     */
    EncodedParameter encodeArray(Supplier<ByteBuf> encodedSupplier, PostgresTypeIdentifier dataType) {
        return create(Format.FORMAT_TEXT, dataType, encodedSupplier);
    }

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return type.isArray() && this.componentType.equals(getBaseComponentType(type));
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [delegate=").append(this.delegate);
        sb.append(']');
        return sb.toString();
    }

    private static Class<?> getBaseComponentType(Class<?> type) {
        Class<?> t = type;

        while (t.getComponentType() != null) {
            t = t.getComponentType();
        }

        return t;
    }

    private static int getDimensions(List<?> list) {
        int dims = 1;

        Object inner = list.get(0);

        while (inner instanceof List) {
            inner = ((List<?>) inner).get(0);
            dims++;
        }

        return dims;
    }

    @SuppressWarnings("unchecked")
    static <T> T[] decodeBinary(ByteBuf buffer, PostgresTypeIdentifier dataType, Decoder<T> decoder, Class<T> componentType, Class<?> returnType) {
        if (!buffer.isReadable()) {
            return (T[]) Array.newInstance(componentType, 0);
        }

        int dimensions = buffer.readInt();
        if (dimensions == 0) {
            return (T[]) Array.newInstance(componentType, 0);
        }

        if (returnType != Object.class) {
            Assert.requireArrayDimension(returnType, dimensions, "Dimensions mismatch: %s expected, but %s returned from DB");
        }

        buffer.skipBytes(4); // flags: 0=no-nulls, 1=has-nulls
        buffer.skipBytes(4); // element oid

        int[] dims = new int[dimensions];
        for (int d = 0; d < dimensions; ++d) {
            dims[d] = buffer.readInt(); // dimension size
            buffer.skipBytes(4); // lower bound ignored
        }

        T[] array = (T[]) Array.newInstance(componentType, dims);

        readArrayAsBinary(buffer, dataType, array, dims, decoder, componentType, 0);

        return array;
    }

    @SuppressWarnings("unchecked")
    static <T> T[] decodeText(ByteBuf buffer, PostgresTypeIdentifier dataType, byte delimiter, Decoder<T> decoder, Class<T> componentType, Class<?> returnType) {
        List<T> elements = (List<T>) decodeText(buffer, delimiter, dataType, decoder, componentType);

        if (elements.isEmpty()) {
            return (T[]) Array.newInstance(componentType, 0);
        }

        int dimensions = getDimensions(elements);

        if (returnType != Object.class) {
            Assert.requireArrayDimension(returnType, dimensions, "Dimensions mismatch: %s expected, but %s returned from DB");
        }

        return toArray(elements, (Class<T>) createArrayType(componentType, dimensions).getComponentType());
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> createArrayType(Class<T> componentType, int dims) {
        int[] size = new int[dims];
        Arrays.fill(size, 1);
        return (Class<T>) Array.newInstance(componentType, size).getClass();
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] toArray(List<T> list, Class<T> returnType) {
        List<Object> result = new ArrayList<>(list.size());

        for (Object e : list) {
            Object o = (e instanceof List ? toArray((List<Object>) e, (Class<Object>) returnType.getComponentType()) : e);
            result.add(o);
        }

        return result.toArray((T[]) Array.newInstance(returnType, list.size()));
    }

    private static <T> List<Object> decodeText(ByteBuf buf, byte delimiter, PostgresTypeIdentifier dataType, Decoder<T> decoder, Class<T> componentType) {
        List<Object> arrayList = new ArrayList<>();

        boolean insideString = false;
        boolean wasInsideString = false; // needed for checking if NULL
        // value occurred
        List<List<Object>> dims = new ArrayList<>(); // array dimension arrays
        List<Object> currentArray = arrayList; // currently processed array

        // Starting with 8.0 non-standard (beginning index
        // isn't 1) bounds the dimensions are returned in the
        // data formatted like so "[0:3]={0,1,2,3,4}".
        // Older versions simply do not return the bounds.
        //
        // Right now we ignore these bounds, but we could
        // consider allowing these index values to be used
        // even though the JDBC spec says 1 is the first
        // index. I'm not sure what a client would like
        // to see, so we just retain the old behavior.

        if (buf.isReadable() && buf.getByte(0) == '[') {
            while (buf.readByte() != '=') {
                //
            }
        }

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
            } else if (!insideString && currentChar == '{') {
                // subarray start
                if (dims.isEmpty()) {
                    dims.add(arrayList);
                } else {
                    List<Object> a = new ArrayList<>();
                    List<Object> p = dims.get(dims.size() - 1);
                    p.add(a);
                    dims.add(a);
                }
                currentArray = dims.get(dims.size() - 1);

                for (int t = indentEscape + 1; t < buf.writerIndex(); t++) {
                    if (!Character.isWhitespace(buf.getByte(t)) && buf.getByte(t) != '{') {
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
            } else if ((!insideString && (currentChar == delimiter || currentChar == '}'))
                || indentEscape == buf.writerIndex() - 1) {
                // array end or element end
                // when character that is a part of array element
                int skipTrailingBytes = 0;
                if (currentChar != '}' && currentChar != delimiter && readFrom > 0) {
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
                            currentArray.add(null);
                        } else {
                            currentArray.add(decoder.decode(slice, dataType, FORMAT_TEXT, componentType));
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

                // when end of an array
                if (currentChar == '}') {
                    dims.remove(dims.size() - 1);

                    // when multi-dimension
                    if (!dims.isEmpty()) {
                        currentArray = dims.get(dims.size() - 1);
                    }
                }
            }
        }

        return arrayList;
    }

    @SuppressWarnings("unchecked")
    private void encodeAsText(ByteBuf byteBuf, Object[] value, Function<T, String> encoder) {
        byteBuf.writeByte(OPEN_CURLY);
        for (int i = 0; i < value.length; i++) {
            Object item = value[i];
            if (item instanceof Object[]) {
                encodeAsText(byteBuf, (Object[]) item, encoder);
            } else {
                byteBuf.writeCharSequence(item == null ? NULL : encoder.apply((T) item), StandardCharsets.UTF_8);
            }

            if (i != value.length - 1) {
                byteBuf.writeByte(delimiter);
            }
        }
        byteBuf.writeByte(CLOSE_CURLY);
    }

    private static <T> void readArrayAsBinary(ByteBuf buffer, PostgresTypeIdentifier dataType, Object[] array, int[] dims, Decoder<T> decoder, Class<T> componentType, int thisDimension) {
        if (thisDimension == dims.length - 1) {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                int len = buffer.readInt();
                if (len == -1) {
                    continue;
                }
                array[i] = decoder.decode(buffer.readSlice(len), dataType, FORMAT_BINARY, componentType);
            }
        } else {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                readArrayAsBinary(buffer, dataType, (Object[]) array[i], dims, decoder, componentType, thisDimension + 1);
            }
        }
    }

}
