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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.NAME;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class StringCodec extends AbstractCodec<String> implements ArrayCodecDelegate<String> {

    static final Codec<String> STRING_DECODER = StringDecoder.INSTANCE;

    static final Codec<String[]> STRING_ARRAY_DECODER = StringArrayDecoder.INSTANCE;

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(BPCHAR, CHAR, TEXT, UNKNOWN, VARCHAR, NAME);

    private final ByteBufAllocator byteBufAllocator;

    private final PostgresTypeIdentifier defaultType;

    private final PostgresTypeIdentifier arrayType;

    StringCodec(ByteBufAllocator byteBufAllocator) {
        this(byteBufAllocator, VARCHAR, VARCHAR_ARRAY);
    }

    StringCodec(ByteBufAllocator byteBufAllocator, PostgresTypeIdentifier defaultType, PostgresTypeIdentifier arrayType) {
        super(String.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.defaultType = Assert.requireNonNull(defaultType, "defaultType must not be null");
        this.arrayType = Assert.requireNonNull(arrayType, "arrayType must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return createNull(FORMAT_TEXT, this.defaultType);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.defaultType == type || SUPPORTED_TYPES.contains(type);
    }

    @Override
    String doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends String> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return ByteBufUtils.decode(buffer);
    }

    @Override
    EncodedParameter doEncode(String value) {
        return doEncode(value, this.defaultType);
    }

    @Override
    EncodedParameter doEncode(String value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return create(FORMAT_TEXT, dataType, () -> ByteBufUtils.encode(this.byteBufAllocator, value));
    }

    @Override
    public String encodeToText(String value) {
        Assert.requireNonNull(value, "value must not be null");

        return ArrayCodec.escapeArrayElement(value);
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return this.arrayType;
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return Stream.concat(Stream.of(this.defaultType), SUPPORTED_TYPES.stream()).map(PostgresTypeIdentifier.class::cast).collect(Collectors.toSet());
    }

    private static class StringDecoder implements Codec<String>, Decoder<String> {

        static final StringDecoder INSTANCE = new StringDecoder();

        StringDecoder() {
        }

        @Override
        public boolean canDecode(int dataType, Format format, Class<?> type) {
            Assert.requireNonNull(type, "type must not be null");
            return type.isAssignableFrom(String.class);
        }

        @Override
        public boolean canEncode(Object value) {
            return false;
        }

        @Override
        public boolean canEncodeNull(Class<?> type) {
            return false;
        }

        @Override
        public String decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends String> type) {
            Assert.requireNonNull(buffer, "byteBuf must not be null");
            return ByteBufUtils.decode(buffer);
        }

        @Override
        public String decode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends String> type) {
            Assert.requireNonNull(buffer, "byteBuf must not be null");
            return ByteBufUtils.decode(buffer);
        }

        @Override
        public EncodedParameter encode(Object value) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public EncodedParameter encode(Object value, int dataType) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public EncodedParameter encodeNull() {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

    }

    private static class StringArrayDecoder implements Codec<String[]> {

        static final StringArrayDecoder INSTANCE = new StringArrayDecoder();

        @Override
        public boolean canDecode(int dataType, Format format, Class<?> type) {
            Assert.requireNonNull(type, "type must not be null");
            return type.isAssignableFrom(String[].class);
        }

        @Override
        public boolean canEncode(Object value) {
            return false;
        }

        @Override
        public boolean canEncodeNull(Class<?> type) {
            return false;
        }

        @Override
        public String[] decode(ByteBuf buffer, int dataType, Format format, Class<? extends String[]> type) {

            Assert.requireNonNull(buffer, "byteBuf must not be null");
            Assert.requireNonNull(format, "format must not be null");
            Assert.requireNonNull(type, "type must not be null");

            if (FORMAT_BINARY == format) {
                return ArrayCodec.decodeBinary(buffer, AbstractCodec.getDataType(dataType), StringDecoder.INSTANCE, String.class, type);
            } else {
                return ArrayCodec.decodeText(buffer, AbstractCodec.getDataType(dataType), ArrayCodec.COMMA, StringDecoder.INSTANCE, String.class, type);
            }
        }

        @Override
        public EncodedParameter encode(Object value) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public EncodedParameter encode(Object value, int dataType) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public EncodedParameter encodeNull() {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

    }

}
