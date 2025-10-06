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

package io.r2dbc.postgresql.message.frontend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCString;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringASCII;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeShort;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The StartupMessage message.
 */
public final class StartupMessage implements FrontendMessage {

    private static final ByteBuf DATABASE = Unpooled.copiedBuffer("database", UTF_8).asReadOnly();

    private static final ByteBuf USER = Unpooled.copiedBuffer("user", UTF_8).asReadOnly();

    private static final Map<String, ByteBuf> STATIC_STRINGS = new HashMap<>();

    static {

        List<String> strings = Arrays.asList("2", "application_name", "client_encoding", "DateStyle", "extra_float_digits", "ISO", "TimeZone", "utf8");

        for (String string : strings) {
            STATIC_STRINGS.put(string, Unpooled.copiedBuffer(string, UTF_8).asReadOnly());
        }
    }

    private final String database;

    private final StartupParameterProvider parameterProvider;

    private final String username;

    /**
     * Create a new message.
     *
     * @param database          the database to connect to. Defaults to the username.
     * @param username          the database username to connect as
     * @param parameterProvider the provider providing database connection options
     * @throws IllegalArgumentException if {@code applicationName} or {@code username} is {@code null}
     */
    public StartupMessage(@Nullable String database, String username, StartupParameterProvider parameterProvider) {
        this.database = database;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.parameterProvider = Assert.requireNonNull(parameterProvider, "parameterProvider must not be null");
    }

    @Override
    public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        return Mono.fromSupplier(() -> {
            ByteBuf out = byteBufAllocator.ioBuffer();

            writeLengthPlaceholder(out);
            writeShort(out, 3, 0);
            writeParameter(out, USER, this.username);

            if (this.database != null) {
                writeParameter(out, DATABASE, this.database);
            }

            ByteBufParameterWriter writer = new ByteBufParameterWriter(out);
            this.parameterProvider.accept(writer);

            writeByte(out, 0);

            return writeSize(out, 0);
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupMessage that = (StartupMessage) o;
        return
            Objects.equals(this.database, that.database) &&
                Objects.equals(this.username, that.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.database, this.username);
    }

    @Override
    public String toString() {
        return "StartupMessage{" +
            "database='" + this.database + '\'' +
            ", username='" + this.username + '\'' +
            '}';
    }

    static void writeParameter(ByteBuf out, ByteBuf key, String value) {
        writeCString(out, key);
        writeCStringUTF8(out, value);
    }

    /**
     * Writer interface to expose startup parameters.
     */
    public interface ParameterWriter {

        /**
         * Write a parameter value along its name.
         *
         * @param key   the parameter name
         * @param value the parameter value
         */
        void write(String key, String value);

    }

    static class ByteBufParameterWriter implements ParameterWriter {

        private final ByteBuf out;

        ByteBufParameterWriter(ByteBuf out) {
            this.out = out;
        }

        @Override
        public void write(String key, String value) {

            ByteBuf binaryKey = STATIC_STRINGS.get(key);
            ByteBuf binaryValue = STATIC_STRINGS.get(value);

            if (binaryKey == null) {
                writeCStringASCII(this.out, key);
            } else {
                writeCString(this.out, binaryKey);
            }

            if (binaryValue == null) {
                writeCStringUTF8(this.out, value);
            } else {
                writeCString(this.out, binaryValue);
            }
        }

    }

    /**
     * Interface that provides startup parameters into a {@link ParameterWriter}.
     */
    public interface StartupParameterProvider {

        /**
         * Provide parameters to a {@link ParameterWriter}.
         *
         * @param writer the writer object accepting parameters
         */
        void accept(ParameterWriter writer);

    }

}
