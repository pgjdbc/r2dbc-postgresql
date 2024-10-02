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
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CodecRegistrar} to register built-in codecs depending on their availability in {@code pg_type}.
 */
public class BuiltinDynamicCodecs implements CodecRegistrar {

    private static final Object EMPTY = new Object();

    enum BuiltinCodec {

        HSTORE("hstore"),
        POSTGIS_GEOMETRY("geometry") {

            private final boolean jtsPresent = isPresent(BuiltinDynamicCodecs.class.getClassLoader(), "org.locationtech.jts.geom.Geometry");

            @Override
            public boolean isSupported() {
                return this.jtsPresent;
            }
        }, VECTOR("vector");

        private final String name;

        BuiltinCodec(String name) {
            this.name = name;
        }

        public Iterable<Codec<?>> createCodec(ByteBufAllocator byteBufAllocator, int oid, int typarray) {

            switch (this) {
                case HSTORE:
                    return Collections.singletonList(new HStoreCodec(byteBufAllocator, oid));
                case POSTGIS_GEOMETRY:
                    return Collections.singletonList(new PostgisGeometryCodec(oid));
                case VECTOR:
                    VectorCodec vectorCodec = new VectorCodec(byteBufAllocator, oid, typarray);
                    List<Codec<?>> codecs = new ArrayList<>(3);
                    codecs.add(vectorCodec);
                    if (typarray != PostgresTypes.NO_SUCH_TYPE) {
                        codecs.add(new VectorCodec.VectorArrayCodec(byteBufAllocator, vectorCodec));
                    }
                    codecs.add(new VectorFloatCodec(byteBufAllocator, oid));
                    return codecs;
                default:
                    throw new UnsupportedOperationException(String.format("Codec %s for OID %d not supported", name(), oid));
            }
        }

        public String getName() {
            return this.name;
        }

        boolean isSupported() {
            return true;
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
                    int typarray = rowMetadata.contains("typarray") ? PostgresqlObjectId.toInt(row.get("typarray", Long.class)) : PostgresTypes.NO_SUCH_TYPE;
                    String typname = row.get("typname", String.class);

                    BuiltinCodec lookup = BuiltinCodec.lookup(typname);
                    if (lookup.isSupported()) {
                        lookup.createCodec(byteBufAllocator, oid, typarray).forEach(registry::addLast);
                    }

                    return EMPTY;
                })
            ).then();
    }

    private PostgresqlStatement createQuery(PostgresqlConnection connection) {
        return connection.createStatement(String.format("SELECT oid, * FROM pg_catalog.pg_type WHERE typname IN (%s)", getPlaceholders()));
    }

    private static String getPlaceholders() {
        return Arrays.stream(BuiltinCodec.values()).map(s -> "'" + s.getName() + "'").collect(Collectors.joining(","));
    }

    private static boolean isPresent(ClassLoader classLoader, String name) {
        try {
            Class.forName(name, false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

}
