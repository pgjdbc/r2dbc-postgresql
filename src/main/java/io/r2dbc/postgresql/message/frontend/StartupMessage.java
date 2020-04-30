/*
 * Copyright 2017-2020 the original author or authors.
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeByte;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCString;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeCStringUTF8;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeLengthPlaceholder;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeShort;
import static io.r2dbc.postgresql.message.frontend.FrontendMessageUtils.writeSize;

/**
 * The StartupMessage message.
 */
public final class StartupMessage implements FrontendMessage {

    private static final ByteBuf APPLICATION_NAME = Unpooled.copiedBuffer("application_name", UTF_8).asReadOnly();

    private static final ByteBuf CLIENT_ENCODING = Unpooled.copiedBuffer("client_encoding", UTF_8).asReadOnly();

    private static final ByteBuf DATABASE = Unpooled.copiedBuffer("database", UTF_8).asReadOnly();

    private static final ByteBuf DATE_STYLE = Unpooled.copiedBuffer("DateStyle", UTF_8).asReadOnly();

    private static final ByteBuf EXTRA_FLOAT_DIGITS = Unpooled.copiedBuffer("extra_float_digits", UTF_8).asReadOnly();

    private static final ByteBuf ISO = Unpooled.copiedBuffer("ISO", UTF_8).asReadOnly();

    private static final ByteBuf NUMERAL_2 = Unpooled.copiedBuffer("2", UTF_8).asReadOnly();

    private static final ByteBuf SYSTEM_TIME_ZONE = Unpooled.copiedBuffer(TimeZone.getDefault().getID(), UTF_8).asReadOnly();

    private static final ByteBuf TIMEZONE = Unpooled.copiedBuffer("TimeZone", UTF_8).asReadOnly();

    private static final ByteBuf USER = Unpooled.copiedBuffer("user", UTF_8).asReadOnly();

    private static final ByteBuf UTF8 = Unpooled.copiedBuffer("utf8", UTF_8).asReadOnly();

    private final String applicationName;

    private final String database;

    private final Map<String, String> options;

    private final String username;

    /**
     * Creates a new message.
     *
     * @param applicationName the name of the application connecting to the database
     * @param database        the database to connect to. Defaults to the user name.
     * @param username        the database user name to connect as
     * @param options         database connection options
     * @throws IllegalArgumentException if {@code applicationName} or {@code username} is {@code null}
     */
    public StartupMessage(String applicationName, @Nullable String database, String username, @Nullable Map<String, String> options) {
        this.applicationName = Assert.requireNonNull(applicationName, "applicationName must not be null");
        this.database = database;
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.options = options;
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

            writeParameter(out, APPLICATION_NAME, this.applicationName);
            writeParameter(out, CLIENT_ENCODING, UTF8);
            writeParameter(out, DATE_STYLE, ISO);
            writeParameter(out, EXTRA_FLOAT_DIGITS, NUMERAL_2);
            writeParameter(out, TIMEZONE, SYSTEM_TIME_ZONE);
            if (this.options != null) {
                for (Map.Entry<String, String> option : this.options.entrySet()) {
                    ByteBuf key = Unpooled.copiedBuffer(option.getKey(), UTF_8);
                    writeParameter(out, key, option.getValue());
                    key.release();
                }
            }
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
        return Objects.equals(this.applicationName, that.applicationName) &&
            Objects.equals(this.database, that.database) &&
            Objects.equals(this.username, that.username) &&
            Objects.equals(this.options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.applicationName, this.database, this.username, this.options);
    }

    @Override
    public String toString() {
        return "StartupMessage{" +
            "applicationName='" + applicationName + '\'' +
            ", database='" + database + '\'' +
            ", username='" + username + '\'' +
            ", options='" + options + '\'' +
            '}';
    }

    private void writeParameter(ByteBuf out, ByteBuf key, String value) {
        writeCString(out, key);
        writeCStringUTF8(out, value);
    }

    private void writeParameter(ByteBuf out, ByteBuf key, ByteBuf value) {
        writeCString(out, key);
        writeCString(out, value);
    }

}
