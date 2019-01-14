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
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;

abstract class AbstractArrayCodec<T> extends AbstractCodec<Object[]> {

    private static final String NULL = "NULL";
    private static final byte[] COMMA = ",".getBytes();
    private static final byte[] OPEN_CURLY = "{".getBytes();
    private static final byte[] CLOSE_CURLY = "}".getBytes();

    private final ByteBufAllocator byteBufAllocator;

    private final Class<T> componentType;

    AbstractArrayCodec(ByteBufAllocator byteBufAllocator, Class<T> componentType) {
        super(Object[].class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.componentType = Assert.requireNonNull(componentType, "componentType must not be null");
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return isTypeAssignable(value.getClass());
    }

    abstract T decodeItem(ByteBuf byteBuf);

    abstract T decodeItem(String strValue);

    @Override
    final Object[] doDecode(ByteBuf byteBuf, Format format, Class<? extends Object[]> type) {
        Assert.requireNonNull(byteBuf, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (FORMAT_BINARY == format) {
            return decodeBinary(byteBuf, type);
        } else {
            return decodeText(byteBuf, type);
        }
    }

    @Override
    final Parameter doEncode(Object[] value) {
        Assert.requireNonNull(value, "value must not be null");

        ByteBuf byteBuf = this.byteBufAllocator.buffer();
        encodeAsText(byteBuf, value, this::encodeItem);

        return encodeArray(byteBuf);
    }

    abstract Parameter encodeArray(ByteBuf byteBuf);

    abstract String encodeItem(T value);

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (!type.isArray()) {
            return false;
        }

        return getBaseComponentType(type).equals(this.componentType);
    }

    static String escapeArrayElement(String s) {
        StringBuilder b = new StringBuilder();
        b.append('"');
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

    private static Class<?> getBaseComponentType(Class<?> type) {
        Class<?> t = type;

        while (t.isArray()) {
            t = t.getComponentType();
        }

        return t;
    }

    private static int getDimensions(List<?> list) {
        int dims = 1;

        Object inner = list.get(0);

        while (inner instanceof List) {
            inner = ((List) inner).get(0);
            dims++;
        }

        return dims;
    }

    private Class<?> createArrayType(int dims) {
        int[] size = new int[dims];
        Arrays.fill(size, 1);
        return Array.newInstance(componentType, size).getClass();
    }

    @SuppressWarnings("unchecked")
    private void encodeAsText(ByteBuf byteBuf, Object[] value, Function<T, String> encoder) {
        byteBuf.writeBytes(OPEN_CURLY);
        for (int i = 0; i < value.length; i++) {
            Object item = value[i];
            if (item instanceof Object[]) {
                encodeAsText(byteBuf, (Object[]) item, encoder);
            } else {
                byteBuf.writeCharSequence(item == null ? NULL : encoder.apply((T) item), StandardCharsets.UTF_8);
            }

            if (i != value.length - 1) {
                byteBuf.writeBytes(COMMA);
            }
        }
        byteBuf.writeBytes(CLOSE_CURLY);
    }

    private void readArrayAsBinary(ByteBuf buffer, Object[] array, int[] dims, int thisDimension) {
        if (thisDimension == dims.length - 1) {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                int len = buffer.readInt();
                if (len == -1) {
                    continue;

                }
                array[i] = decodeItem(buffer.readBytes(len));
            }
        } else {
            for (int i = 0; i < dims[thisDimension]; ++i) {
                readArrayAsBinary(buffer, (Object[]) array[i], dims, thisDimension + 1);
            }
        }
    }

    private Object[] decodeBinary(ByteBuf buffer, Class<?> returnType) {
        int dimensions = buffer.readInt();
        if (dimensions == 0) {
            return (Object[]) Array.newInstance(componentType, 0);
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

        Object[] array = (Object[]) Array.newInstance(componentType, dims);

        readArrayAsBinary(buffer, array, dims, 0);

        return array;
    }

    private Object[] decodeText(ByteBuf buffer, Class<?> returnType) {
        List<?> elements = buildArrayList(buffer);

        if (elements.isEmpty()) {
            return (Object[]) Array.newInstance(componentType, 0);
        }

        int dimensions = getDimensions(elements);

        if (returnType != Object.class) {
            Assert.requireArrayDimension(returnType, dimensions, "Dimensions mismatch: %s expected, but %s returned from DB");
        }

        return toArray(elements, createArrayType(dimensions).getComponentType());
    }

    private static Object[] toArray(List<?> list, Class<?> returnType) {
        return list
                .stream()
                .map(e -> (e instanceof List ? toArray((List) e, returnType.getComponentType()) : e))
                .toArray(r ->  (Object[]) Array.newInstance(returnType, list.size()));
    }

    private List<Object> buildArrayList(ByteBuf buf) {
        List<Object> arrayList = new ArrayList<>();

        char delim = ','; // todo parametrize

        StringBuilder buffer = null;
        boolean insideString = false;
        boolean wasInsideString = false; // needed for checking if NULL
        // value occurred
        List<List<Object>> dims = new ArrayList<>(); // array dimension arrays
        List<Object> curArray = arrayList; // currently processed array

        CharSequence chars = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8);

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
        int startOffset = 0;

        {
            if (chars.charAt(0) == '[') {
                while (chars.charAt(startOffset) != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }
        }

        char currentChar;

        for (int i = startOffset; i < chars.length(); i++) {
            currentChar = chars.charAt(i);
            // escape character that we need to skip
            if (currentChar == '\\') {
                i++;
                currentChar = chars.charAt(i);
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
                curArray = dims.get(dims.size() - 1);

                for (int t = i + 1; t < chars.length(); t++) {
                    if (!Character.isWhitespace(chars.charAt(t)) && chars.charAt(t) != '{') {
                        break;
                    }
                }

                buffer = new StringBuilder();
                continue;
            } else if (currentChar == '"') {
                // quoted element
                insideString = !insideString;
                wasInsideString = true;
                continue;
            } else if (!insideString && Character.isWhitespace(currentChar)) {
                // white space
                continue;
            } else if ((!insideString && (currentChar == delim || currentChar == '}'))
                    || i == chars.length() - 1) {
                // array end or element end
                // when character that is a part of array element
                if (currentChar != '"' && currentChar != '}' && currentChar != delim && buffer != null) {
                    buffer.append(currentChar);
                }

                String b = buffer == null ? null : buffer.toString();

                // add element to current array
                if (b != null && (!b.isEmpty() || wasInsideString)) {
                    curArray.add(!wasInsideString && b.equals("NULL") ? null : decodeItem(b));
                }

                wasInsideString = false;
                buffer = new StringBuilder();

                // when end of an array
                if (currentChar == '}') {
                    dims.remove(dims.size() - 1);

                    // when multi-dimension
                    if (!dims.isEmpty()) {
                        curArray = dims.get(dims.size() - 1);
                    }

                    buffer = null;
                }

                continue;
            }

            if (buffer != null) {
                buffer.append(currentChar);
            }
        }

        return arrayList;
    }
}
