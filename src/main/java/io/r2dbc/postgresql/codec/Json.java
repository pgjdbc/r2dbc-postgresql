/*
 * Copyright 2019-2020 the original author or authors.
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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Value object to represent JSON values.
 * <p>JSON values are divided into input and output values.
 * Input values are provided by code that wants to bind values to a query.  Input values are intended to be consumed by the driver.
 * Input values should not be consumed by application code.
 *
 * <p>Output values are returned by the driver to be consumed by application code using the following methods:
 * <ul>
 *     <li>{@link #mapBuffer(Function)}</li>
 *     <li>{@link #mapByteBuf(Function)}</li>
 *     <li>{@link #mapInputStream(Function)}</li>
 *     <li>{@link #asString()}</li>
 *     <li>{@link #asArray()}</li>
 * </ul>
 * <p>
 * JSON values should be generally considered for single-consumption only.  Output values retain a reference to a potentially pooled memory buffer and must be consumed to avoid memory leaks.
 */
public abstract class Json {

    Json() {
    }

    /**
     * Creates a {@link Json} object from a {@link ByteBuffer}.
     *
     * @param buffer the JSON value as {@link ByteBuffer}
     * @return {@link Json} object from a {@link ByteBuffer}.
     * @throws IllegalArgumentException if {@code buffer} is {@code null}
     */
    public static Json of(ByteBuffer buffer) {
        return new JsonByteBufInput(Unpooled.wrappedBuffer(Assert.requireNonNull(buffer, "buffer must not be null")));
    }

    /**
     * Creates a {@link Json} object from a {@link ByteBuf}.
     * <p>The {@link ByteBuf} is released after encoding the value.
     *
     * @param buffer the JSON value as {@link ByteBuf}
     * @return {@link Json} object from a {@link ByteBuf}.
     * @throws IllegalArgumentException if {@code buffer} is {@code null}
     */
    public static Json of(ByteBuf buffer) {
        return new JsonByteBufInput(Assert.requireNonNull(buffer, "buffer must not be null"));
    }

    /**
     * Creates a {@link Json} object from a {@link InputStream}.
     * <p>The {@link InputStream} is {@link InputStream#close() closed} after encoding the value.
     *
     * @param inputStream the JSON value as {@link InputStream}
     * @return {@link Json} object from a {@link InputStream}.
     * @throws IllegalArgumentException if {@code inputStream} is {@code null}
     */
    public static Json of(InputStream inputStream) {
        return new JsonInputStreamInput(Assert.requireNonNull(inputStream, "inputStream must not be null"));
    }

    /**
     * Creates a {@link Json} object from a {@code byte[] value}.
     *
     * @param value the JSON value as {@code byte[]}
     * @return {@link Json} object from a {@code byte[] value}.
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static Json of(byte[] value) {
        return new JsonByteArrayInput(Assert.requireNonNull(value, "buffer must not be null"));
    }

    /**
     * Creates a {@link Json} object from a {@link String}. Uses UTF-8 encoding to convert the value into its binary representation.
     *
     * @param value the JSON value as {@link String}
     * @return {@link Json} object from a {@link String}.
     * @throws IllegalArgumentException if {@code value} is {@code null}
     */
    public static Json of(String value) {
        return new JsonByteArrayInput(Assert.requireNonNull(value, "value must not be null").getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns an object consisting of the result of applying the given
     * mapping {@link Function} to the {@link ByteBuffer} of this JSON value.
     * <p>Consumption methods should be called exactly once as the underlying JSON value is typically released after consumption.
     *
     * @param mappingFunction mapping function that gets applied to the {@link ByteBuffer} representation of this JSON value.
     * @param <T>             return type.
     * @return the mapped value. Can be {@code null}.
     */
    @Nullable
    public abstract <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction);

    /**
     * Returns an object consisting of the result of applying the given
     * mapping {@link Function} to the {@link ByteBuf} of this JSON value.
     * <p>Consumption methods should be called exactly once as the underlying JSON value is typically released after consumption.
     *
     * @param mappingFunction mapping function that gets applied to the {@link ByteBuf} representation of this JSON value.
     * @param <T>             return type.
     * @return the mapped value. Can be {@code null}.
     */
    @Nullable
    public abstract <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction);

    /**
     * Returns an object consisting of the result of applying the given
     * mapping {@link Function} to the {@link InputStream} of this JSON value.
     * <p>Consumption methods should be called exactly once as the underlying JSON value is typically released after consumption.
     *
     * @param mappingFunction mapping function that gets applied to the {@link InputStream} representation of this JSON value.
     * @param <T>             return type.
     * @return the mapped value. Can be {@code null}.
     */
    @Nullable
    public abstract <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction);

    /**
     * Returns the value as {@code byte[]}.
     * <p>Consumption methods should be called exactly once as the underlying JSON value is typically released after consumption.
     *
     * @return the contents of the JSON value as {@code byte[]}.
     */
    public abstract byte[] asArray();

    /**
     * Returns the value as {@link String}.
     * <p>Consumption methods should be called exactly once as the underlying JSON value is typically released after consumption.
     *
     * @return the contents of the JSON value as {@link String}.
     */
    public abstract String asString();

    /**
     * JSON input value.
     */
    static abstract class JsonInput<T> extends Json {

        final T value;

        JsonInput(T value) {
            this.value = value;
        }

    }

    /**
     * JSON input as {@link ByteBuf}.
     */
    static final class JsonByteBufInput extends JsonInput<ByteBuf> {

        JsonByteBufInput(ByteBuf value) {
            super(value);
        }

        @Override
        public <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(this.value.nioBuffer());
        }

        @Override
        public <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(this.value);
        }

        @Override
        public <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(new ByteBufInputStream(this.value));
        }

        @Override
        public byte[] asArray() {
            return ByteBufUtil.getBytes(this.value);
        }

        @Override
        public String asString() {
            return this.value.toString(0, this.value.readableBytes(), StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return "JsonByteBufInput{" +
                asString() +
                '}';
        }

    }

    /**
     * JSON input as {@code byte[]}.
     */
    static final class JsonByteArrayInput extends JsonInput<byte[]> {

        JsonByteArrayInput(byte[] value) {
            super(value);
        }

        @Override
        public <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(ByteBuffer.wrap(asArray()));
        }

        @Override
        public <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(Unpooled.wrappedBuffer(asArray()));
        }

        @Override
        public <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(new ByteArrayInputStream(asArray()));
        }

        @Override
        public byte[] asArray() {
            return this.value;
        }

        @Override
        public String asString() {
            return new String(this.value);
        }

        @Override
        public String toString() {
            return "JsonByteBufInput{" +
                asString() +
                '}';
        }

    }

    /**
     * JSON input as {@link InputStream}.
     */
    static final class JsonInputStreamInput extends JsonInput<InputStream> {

        JsonInputStreamInput(InputStream value) {
            super(value);
        }

        @Override
        public <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(ByteBuffer.wrap(asArray()));
        }

        @Override
        public <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(Unpooled.wrappedBuffer(asArray()));
        }

        @Override
        public <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "mappingFunction must not be null");
            return mappingFunction.apply(this.value);
        }

        @Override
        public byte[] asArray() {

            if (this.value.markSupported()) {
                this.value.mark(Integer.MAX_VALUE);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int bytesRead;

            try {
                while ((bytesRead = this.value.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }

                if (this.value.markSupported()) {
                    this.value.reset();
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read bytes from InputStream", e);
            }

            return out.toByteArray();
        }

        @Override
        public String asString() {
            return new String(asArray(), StandardCharsets.UTF_8);
        }

    }

    /**
     * JSON output value.
     */
    static final class JsonOutput extends Json {

        final ByteBuf buffer;

        volatile boolean released;

        JsonOutput(ByteBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte[] asArray() {
            assertNotReleased();

            try {
                byte[] bytes = new byte[this.buffer.readableBytes()];
                this.buffer.readBytes(bytes);
                return bytes;
            } finally {
                release();
            }
        }

        @Override
        public String asString() {
            return new String(asArray());
        }

        void assertNotReleased() {
            if (this.released) {
                throw new IllegalStateException("JSON is already released");
            }
        }

        @Override
        public <T> T mapBuffer(Function<ByteBuffer, ? extends T> mappingFunction) {
            assertNotReleased();

            ByteBuffer buffer = ByteBuffer.allocate(this.buffer.readableBytes());
            this.buffer.readBytes(buffer);
            buffer.flip();
            release();

            return mappingFunction.apply(buffer);
        }

        @Override
        public <T> T mapByteBuf(Function<ByteBuf, ? extends T> mappingFunction) {
            assertNotReleased();

            try {
                return mappingFunction.apply(this.buffer);
            } finally {
                release();
            }
        }

        @Override
        public <T> T mapInputStream(Function<InputStream, ? extends T> mappingFunction) {
            assertNotReleased();

            try {
                return mappingFunction.apply(new ByteBufInputStream(this.buffer));
            } finally {
                release();
            }
        }

        private void release() {
            this.released = true;
            this.buffer.release();
        }

        @Override
        public String toString() {

            if (this.released) {
                return "JsonOutput{[released]}";
            }

            return "JsonOutput{" +
                this.buffer.toString(StandardCharsets.UTF_8)
                + "}";

        }

    }

}
