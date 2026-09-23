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
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CodecRegistrar} to register built-in codecs depending on their availability in {@code pg_type}.
 */
public class BuiltinDynamicCodecs implements CodecRegistrar {

    enum BuiltinCodec {

        HSTORE("hstore"),
        POSTGIS_GEOMETRY("geometry") {

            private final boolean jtsPresent = isPresent(BuiltinDynamicCodecs.class.getClassLoader(), "org.locationtech.jts.geom.Geometry");

            @Override
            public boolean isSupported() {
                return this.jtsPresent;
            }
        },
        POSTGIS_GEOGRAPHY("geography") {

            private final boolean jtsPresent = isPresent(BuiltinDynamicCodecs.class.getClassLoader(), "org.locationtech.jts.geom.Geometry");

            @Override
            public boolean isSupported() {
                return this.jtsPresent;
            }
        },

        VECTOR("vector");

        private final String name;

        BuiltinCodec(String name) {
            this.name = name;
        }

        public Iterable<Codec<?>> createCodec(ByteBufAllocator byteBufAllocator, int oid, int typarray) {

            switch (this) {
                case HSTORE:
                    return Collections.singletonList(new HStoreCodec(byteBufAllocator, oid));
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

                    String typname = row.get("typname", String.class);
                    BuiltinCodec lookup = BuiltinCodec.lookup(typname);
                    int oid = PostgresqlObjectId.toInt(row.get("oid", Long.class));
                    int typarray = rowMetadata.contains("typarray") ? PostgresqlObjectId.toInt(row.get("typarray", Long.class)) : PostgresTypes.NO_SUCH_TYPE;

                    return new DiscoveredType(lookup, oid, typarray);
                })
            )
            .collectList()
            .doOnNext(types -> registerCodecs(types, byteBufAllocator, registry))
            .then();
    }

    void registerCodecs(List<DiscoveredType> types, ByteBufAllocator byteBufAllocator, CodecRegistry registry) {

        int geometryOid = PostgresTypes.NO_SUCH_TYPE;
        int geographyOid = PostgresTypes.NO_SUCH_TYPE;

        for (DiscoveredType discovered : types) {

            if (!discovered.codec.isSupported()) {
                continue;
            }

            switch (discovered.codec) {
                case POSTGIS_GEOMETRY:
                    geometryOid = discovered.oid;
                    continue;
                case POSTGIS_GEOGRAPHY:
                    geographyOid = discovered.oid;
                    continue;
                default:
                    discovered.codec.createCodec(byteBufAllocator, discovered.oid, discovered.typarray).forEach(registry::addLast);
            }
        }

        // geometry and geography share a single codec so that a plain JTS Geometry is unambiguous for encoding; Postgres applies its own
        // geometry -> geography implicit cast wherever a geography value is actually required.
        if (geometryOid != PostgresTypes.NO_SUCH_TYPE) {
            registry.addLast(new PostgisCodec(geometryOid, geographyOid));
        }
    }

    static final class DiscoveredType {

        private final BuiltinCodec codec;

        private final int oid;

        private final int typarray;

        DiscoveredType(BuiltinCodec codec, int oid, int typarray) {
            this.codec = codec;
            this.oid = oid;
            this.typarray = typarray;
        }

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
