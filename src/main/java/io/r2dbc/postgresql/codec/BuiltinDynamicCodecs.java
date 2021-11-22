/*
 * Copyright 2020 the original author or authors.
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

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * {@link CodecRegistrar} to register built-in codecs depending on their availability in {@code pg_type}.
 */
public class BuiltinDynamicCodecs implements CodecRegistrar {

    private static final Object EMPTY = new Object();

    enum BuiltinCodec {

        HSTORE("hstore");

        private final String name;

        BuiltinCodec(String name) {
            this.name = name;
        }

        public Codec<?> createCodec(ByteBufAllocator byteBufAllocator, int oid) {

            switch (this) {
                case HSTORE:
                    return new HStoreCodec(byteBufAllocator, oid);
                default:
                    throw new UnsupportedOperationException(String.format("Codec %s for OID %d not supported", name(), oid));
            }
        }

        public String getName() {
            return this.name;
        }

        static BuiltinCodec lookup(@Nullable String name) {

            for (BuiltinCodec codec : values()) {
                if (codec.getName().equalsIgnoreCase(name)) {
                    return codec;
                }
            }

            throw new IllegalArgumentException(String.format("Cannot determine codec for %s", name));
        }
    }

    @Override
    public Publisher<Void> register(PostgresqlConnection connection, ByteBufAllocator byteBufAllocator, CodecRegistry registry) {

        PostgresqlStatement statement = createQuery(connection);

        return statement.execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {

                int oid = PostgresqlObjectId.toInt(row.get("oid", Long.class));
                String typname = row.get("typname", String.class);

                    BuiltinCodec lookup = BuiltinCodec.lookup(typname);
                    registry.addLast(lookup.createCodec(byteBufAllocator, oid));

                    return EMPTY;
                })
            ).then();
    }

    private PostgresqlStatement createQuery(PostgresqlConnection connection) {
        return connection.createStatement(String.format("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname IN (%s)", getPlaceholders()));
    }

    private static String getPlaceholders() {
        return Arrays.stream(BuiltinCodec.values()).map(s -> "'" + s.getName() + "'").collect(Collectors.joining(","));
    }

}
