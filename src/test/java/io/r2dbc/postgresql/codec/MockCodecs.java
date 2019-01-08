/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class MockCodecs implements Codecs {

    private final Map<Decoding, Object> decodings;

    private final Map<Object, Parameter> encodings;

    private MockCodecs(Map<Decoding, Object> decodings, Map<Object, Parameter> encodings) {
        this.decodings = Assert.requireNonNull(decodings, "decodings must not be null");
        this.encodings = Assert.requireNonNull(encodings, "encodings must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static MockCodecs empty() {
        return builder().build();
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T decode(@Nullable ByteBuf byteBuf, int dataType, Format format, Class<? extends T> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        Decoding decoding = new Decoding(byteBuf, dataType, format, type);

        if (!this.decodings.containsKey(decoding)) {
            throw new AssertionError(String.format("Unexpected call to decode(ByteBuf,int,Format,Class<?>) with values '%s, %d, %s, %s'", byteBuf, dataType, format, type.getName()));
        }

        return (T) this.decodings.get(decoding);
    }

    @Override
    public Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        if (!this.encodings.containsKey(value)) {
            throw new AssertionError(String.format("Unexpected call to encode(Object) with value '%s'", value));
        }

        return this.encodings.get(value);
    }

    @Override
    public Parameter encodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        if (!this.encodings.containsKey(type)) {
            throw new AssertionError(String.format("Unexpected call to encodeNull(Class<?>) with value '%s'", type));
        }

        return this.encodings.get(type);
    }

    @Override
    public String toString() {
        return "MockCodecs{" +
            "decodings=" + this.decodings +
            ", encodings=" + this.encodings +
            '}';
    }

    public static final class Builder {

        private final Map<Decoding, Object> decodings = new HashMap<>();

        private final Map<Object, Parameter> encodings = new HashMap<>();

        private Builder() {
        }

        public MockCodecs build() {
            return new MockCodecs(this.decodings, this.encodings);
        }

        public <T> Builder decoding(@Nullable ByteBuf byteBuf, int dataType, Format format, Class<T> type, T value) {
            Assert.requireNonNull(format, "format must not be null");
            Assert.requireNonNull(type, "type must not be null");

            this.decodings.put(new Decoding(byteBuf, dataType, format, type), value);
            return this;
        }

        public Builder encoding(@Nullable Object value, Parameter parameter) {
            Assert.requireNonNull(parameter, "parameter must not be null");

            this.encodings.put(value, parameter);
            return this;
        }

        @Override
        public String toString() {
            return "Builder{" +
                "decodings=" + this.decodings +
                ", encodings=" + this.encodings +
                '}';
        }

    }

    private static final class Decoding {

        private final ByteBuf byteBuf;

        private final int dataType;

        private final Format format;

        private final Class<?> type;

        private Decoding(@Nullable ByteBuf byteBuf, int dataType, Format format, Class<?> type) {
            this.byteBuf = byteBuf;
            this.dataType = dataType;
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
            Decoding that = (Decoding) o;
            return this.dataType == that.dataType &&
                Objects.equals(this.byteBuf, that.byteBuf) &&
                this.format == that.format &&
                Objects.equals(this.type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.byteBuf, this.dataType, this.format, this.type);
        }

        @Override
        public String toString() {
            return "Decoding{" +
                "byteBuf=" + this.byteBuf +
                ", dataType=" + this.dataType +
                ", format=" + this.format +
                ", type=" + this.type +
                '}';
        }

    }

}
