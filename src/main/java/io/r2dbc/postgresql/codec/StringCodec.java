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
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.util.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NAME;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;

final class StringCodec extends AbstractCodec<String> {

    static final Codec<String> STRING_DECODER = StringDecoder.INSTANCE;

    static final Codec<String[]> STRING_ARRAY_DECODER = StringArrayDecoder.INSTANCE;

    private static final Set<PostgresqlObjectId> SUPPORTED_TYPES = EnumSet.of(BPCHAR, CHAR, TEXT, UNKNOWN, VARCHAR, NAME);

    private final ByteBufAllocator byteBufAllocator;

    StringCodec(ByteBufAllocator byteBufAllocator) {
        super(String.class);
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    }

    @Override
    public Parameter encodeNull() {
        return createNull(VARCHAR, FORMAT_TEXT);
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return SUPPORTED_TYPES.contains(type);
    }

    @Override
    String doDecode(ByteBuf buffer, PostgresqlObjectId dataType, @Nullable Format format, @Nullable Class<? extends String> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        return ByteBufUtils.decode(buffer);
    }

    @Override
    Parameter doEncode(String value) {
        Assert.requireNonNull(value, "value must not be null");

        return create(VARCHAR, FORMAT_TEXT, () -> ByteBufUtils.encode(this.byteBufAllocator, value));
    }

    @Override
    public Iterable<PostgresqlObjectId> getDataTypes() {
        return SUPPORTED_TYPES;
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
        public Parameter encode(Object value) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public Parameter encodeNull() {
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
                return ArrayCodec.decodeBinary(buffer, dataType, StringDecoder.INSTANCE, String.class, type);
            } else {
                return ArrayCodec.decodeText(buffer, dataType, ArrayCodec.COMMA, StringDecoder.INSTANCE, String.class, type);
            }
        }

        @Override
        public Parameter encode(Object value) {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

        @Override
        public Parameter encodeNull() {
            throw new UnsupportedOperationException("Cannot encode using a generic enum codec");
        }

    }

}
