/*
 * Copyright 2021 the original author or authors.
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
import io.r2dbc.postgresql.util.Assert;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Generic support class that provides a basis decoding codecs.
 *
 * @since 0.8.11
 */
class ArrayCodec {

    static final byte COMMA = ',';

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
    static <T> T[] decodeBinary(ByteBuf buffer, int dataType, Decoder<T> decoder, Class<T> componentType, Class<?> returnType) {
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
    static <T> T[] decodeText(ByteBuf buffer, int dataType, byte delimiter, Decoder<T> decoder, Class<T> componentType, Class<?> returnType) {
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

    private static <T> List<Object> decodeText(ByteBuf buf, byte delimiter, int dataType, Decoder<T> decoder, Class<T> componentType) {
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

    private static <T> void readArrayAsBinary(ByteBuf buffer, int dataType, Object[] array, int[] dims, Decoder<T> decoder, Class<T> componentType, int thisDimension) {
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
