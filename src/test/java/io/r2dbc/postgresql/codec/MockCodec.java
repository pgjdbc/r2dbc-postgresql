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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Mock imoplementation of {@link Codec}.
 *
 * @param <T>
 */
public final class MockCodec<T> extends AbstractCodec<T> {

    private final Set<CanDecode> canDecodes;

    private final Map<Decoding, T> decodings;

    private final Map<T, EncodedParameter> encodings;

    private MockCodec(Set<CanDecode> canDecodes, Map<Decoding, T> decodings, Map<T, EncodedParameter> encodings, Class<T> type) {
        super(type);

        this.canDecodes = Assert.requireNonNull(canDecodes, "canDecodes must not be null");
        this.decodings = Assert.requireNonNull(decodings, "decodings must not be null");
        this.encodings = Assert.requireNonNull(encodings, "encodings must not be null");
    }

    public static <T> Builder<T> builder(Class<T> type) {
        return new Builder<>(type);
    }

    public static <T> MockCodec<T> empty(Class<T> type) {
        return builder(type).build();
    }

    @Override
    public EncodedParameter encodeNull() {
        return this.encodings.get(null);
    }

    @Override
    public String toString() {
        return "MockCodec{" +
            "canDecodes=" + canDecodes +
            ", decodings=" + decodings +
            ", encodings=" + encodings +
            "} " + super.toString();
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.canDecodes.contains(new CanDecode(format, type));
    }

    @Override
    T doDecode(ByteBuf buffer, PostgresqlObjectId dataType, Format format, Class<? extends T> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        Decoding decoding = new Decoding(buffer, format);

        if (!this.decodings.containsKey(decoding)) {
            throw new AssertionError(String.format("Unexpected call to decode(ByteBuf, Format, Class) with values '%s', '%s', '%s'", buffer, format, type.getName()));
        }

        return this.decodings.get(decoding);

    }

    @Override
    EncodedParameter doEncode(T value) {
        if (!this.encodings.containsKey(value)) {
            throw new AssertionError(String.format("Unexpected call to doEncode(T) with value '%s'", value));
        }

        return this.encodings.get(value);
    }

    @Override
    EncodedParameter doEncode(T value, PostgresTypeIdentifier dataType) {
        if (!this.encodings.containsKey(value)) {
            throw new AssertionError(String.format("Unexpected call to doEncode(T) with value '%s'", value));
        }

        return this.encodings.get(value);
    }

    public static final class Builder<T> {

        private final Set<CanDecode> canDecodes = new HashSet<>();

        private final Map<Decoding, T> decodings = new HashMap<>();

        private final Map<T, EncodedParameter> encodings = new HashMap<>();

        private final Class<T> type;

        private Builder(Class<T> type) {
            this.type = type;
        }

        public MockCodec<T> build() {
            return new MockCodec<>(this.canDecodes, this.decodings, this.encodings, this.type);
        }

        public Builder<T> canDecode(Format format, PostgresqlObjectId type) {
            Assert.requireNonNull(format, "format must not be null");
            Assert.requireNonNull(type, "type must not be null");

            this.canDecodes.add(new CanDecode(format, type));
            return this;
        }

        public Builder<T> decoding(ByteBuf byteBuf, Format format, T value) {
            Assert.requireNonNull(byteBuf, "byteBuf must not be null");
            Assert.requireNonNull(format, "format must not be null");
            Assert.requireNonNull(value, "value must not be null");

            this.decodings.put(new Decoding(byteBuf, format), value);
            return this;
        }

        public Builder<T> encoding(T value, EncodedParameter parameter) {
            Assert.requireNonNull(value, "value must not be null");
            Assert.requireNonNull(parameter, "parameter must not be null");

            this.encodings.put(value, parameter);
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "decodings=" + this.decodings +
                ", encodings=" + this.encodings +
                ", type=" + this.type +
                '}';
        }

    }

    private static final class CanDecode {

        private final Format format;

        private final PostgresqlObjectId type;

        private CanDecode(Format format, PostgresqlObjectId type) {
            this.format = Assert.requireNonNull(format, "format must not be null");
            this.type = Assert.requireNonNull(type, "type must not be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CanDecode that = (CanDecode) o;
            return this.format == that.format &&
                this.type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.format, this.type);
        }

        @Override
        public String toString() {
            return "CanDecode{" +
                "format=" + this.format +
                ", type=" + this.type +
                '}';
        }

    }

    private static final class Decoding {

        private final ByteBuf byteBuf;

        private final Format format;

        private Decoding(ByteBuf byteBuf, Format format) {
            this.byteBuf = Assert.requireNonNull(byteBuf, "byteBuf must not be null");
            this.format = Assert.requireNonNull(format, "format must not be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Decoding that = (Decoding) o;
            return Objects.equals(this.byteBuf, that.byteBuf) &&
                this.format == that.format;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.byteBuf, this.format);
        }

        @Override
        public String toString() {
            return "Decoding{" +
                "byteBuf=" + this.byteBuf +
                ", format=" + this.format +
                '}';
        }

    }

}
