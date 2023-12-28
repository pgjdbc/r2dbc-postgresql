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

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Utility to look up Postgres types using {@code pg_type}.
 *
 * @since 0.8.4
 */
public class PostgresTypes {

    public final static int NO_SUCH_TYPE = -1;

    // parameterized with %s for the comparator (=, IN), %s for the actual criteria value and %s for a potential LIMIT 1 statement
    private static final String SELECT_PG_TYPE = "SELECT pg_type.oid, typarray, typname, typcategory "
        + "  FROM pg_catalog.pg_type "
        + "  LEFT "
        + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
        + "          from pg_namespace as ns "
        + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
        + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
        + "         using ( nspname ) "
        + "       ) as sp "
        + "    ON sp.nspoid = typnamespace "
        + " WHERE typname %s %s "
        + " ORDER BY sp.r, pg_type.oid DESC %s;";

    private final static Pattern TYPENAME = Pattern.compile("[a-zA-Z0-9_]+");

    private final PostgresqlConnection connection;

    private PostgresTypes(PostgresqlConnection connection) {
        this.connection = connection;
    }

    public static PostgresTypes from(PostgresqlConnection connection) {
        return new PostgresTypes(Assert.requireNonNull(connection, "connection must not be null"));
    }

    /**
     * Lookup Postgres types by {@code typname}. Please note that {@code typname} inlined to use simple statements. Therefore, {@code typname} gets verified against {@link #TYPENAME} to prevent SQL
     * injection.
     *
     * @param typeName the type name. Must comply with the pattern {@code [a-zA-Z0-9_]+}
     * @return a mono emitting the {@link PostgresType} if found or {@link Mono#empty()}  if not found
     */
    public Mono<PostgresType> lookupType(String typeName) {
        if (!TYPENAME.matcher(Assert.requireNonNull(typeName, "typeName must not be null")).matches()) {
            throw new IllegalArgumentException(String.format("Invalid typename %s", typeName));
        }

        return this.connection.createStatement(String.format(SELECT_PG_TYPE, "=", "'" + typeName + "'", "LIMIT 1")).execute()
            .flatMap(it -> it.map(PostgresTypes::createType)).singleOrEmpty();
    }

    public Flux<PostgresType> lookupTypes(Iterable<String> typeNames) {

        StringJoiner joiner = new StringJoiner(",", "(", ")");
        AtomicBoolean hasType = new AtomicBoolean();

        typeNames.forEach(typeName -> {

            if (!TYPENAME.matcher(Assert.requireNonNull(typeName, "typeName must not be null")).matches()) {
                throw new IllegalArgumentException(String.format("Invalid typename %s", typeName));
            }

            hasType.set(true);
            joiner.add("'" + typeName + "'");
        });

        if (!hasType.get()) {
            return Flux.empty();
        }

        return this.connection.createStatement(String.format(SELECT_PG_TYPE, "IN", joiner, "")).execute()
            .flatMap(it -> it.map(PostgresTypes::createType));
    }

    private static PostgresType createType(Row row, RowMetadata rowMetadata) {
        Long oid = row.get("oid", Long.class);
        String typname = row.get("typname", String.class);
        String typcategory = row.get("typcategory", String.class);
        Long typarrayOid = rowMetadata.contains("typarray") ? row.get("typarray", Long.class) : null;

        long unsignedTyparray = typarrayOid != null ? typarrayOid : NO_SUCH_TYPE;
        int typarray = typarrayOid != null ? PostgresqlObjectId.toInt(typarrayOid) : NO_SUCH_TYPE;
        return new PostgresType(PostgresqlObjectId.toInt(oid), oid, typarray, unsignedTyparray, typname, typcategory);
    }

    public static class PostgresType implements Type, PostgresTypeIdentifier {

        private final int oid;

        private final long unsignedOid;

        private final int typarray;

        private final long unsignedTyparray;

        private final String name;

        private final String category;

        @Nullable
        private final PostgresqlObjectId objectId;

        public PostgresType(int oid, long unsignedOid, int typarray, long unsignedTyparray, String name, String category) {
            this.oid = oid;
            this.unsignedOid = unsignedOid;
            this.typarray = typarray;
            this.unsignedTyparray = unsignedTyparray;
            this.name = name;
            this.category = category;
            this.objectId = PostgresqlObjectId.isValid(oid) ? PostgresqlObjectId.valueOf(oid) : null;
        }

        @Override
        public int getObjectId() {
            return getOid();
        }

        /**
         * Return the type as array type.
         *
         * @return this type as array type.
         */
        public PostgresType asArrayType() {

            if (isArray()) {
                return this;
            }

            if (this.typarray > 0) {

                return new PostgresType(this.typarray, this.unsignedTyparray, this.typarray, this.unsignedTyparray, this.name, this.category);
            }

            throw new IllegalStateException("No array type available for " + this);
        }

        public int getOid() {
            return this.oid;
        }

        @Override
        public Class<?> getJavaType() {
            return this.objectId != null ? this.objectId.getJavaType() : Object.class;
        }

        public int getArrayObjectId() {
            return this.typarray;
        }

        public long getUnsignedObjectId() {
            return this.unsignedOid;
        }

        public long getUnsignedArrayObjectId() {
            return this.unsignedTyparray;
        }

        public String getName() {
            return this.name;
        }

        /**
         * @return {@code true} if the type is an array type (category code {@code A})
         */
        public boolean isArray() {
            return "A".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a boolean type (category code {@code B})
         */
        public boolean isBoolean() {
            return "B".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a composite type (category code {@code C})
         */
        public boolean isComposite() {
            return "C".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a date/time type (category code {@code D})
         */
        public boolean isDateTime() {
            return "D".equals(this.category);
        }

        /**
         * @return {@code true} if the type is an enum type (category code {@code E})
         */
        public boolean isEnum() {
            return "E".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a geometric type (category code {@code G})
         */
        public boolean isGeometric() {
            return "G".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a network address type (category code {@code I})
         */
        public boolean isNetworkAddress() {
            return "I".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a numeric type (category code {@code N})
         */
        public boolean isNumeric() {
            return "N".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a pseudo-type (category code {@code P})
         */
        public boolean isPseudo() {
            return "P".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a string type (category code {@code S})
         */
        public boolean isString() {
            return "S".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a timespan type (category code {@code T})
         */
        public boolean isTimespan() {
            return "T".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a user-defined type (category code {@code U})
         */
        public boolean isUserDefinedType() {
            return "U".equals(this.category);
        }

        /**
         * @return {@code true} if the type is a bit-string type (category code {@code V})
         */
        public boolean isBitString() {
            return "V".equals(this.category);
        }

        /**
         * @return {@code true} if the type is an unknown type (category code {@code X})
         */
        public boolean isUnknown() {
            return "X".equals(this.category);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PostgresType)) {
                return false;
            }
            PostgresType that = (PostgresType) o;
            return this.oid == that.oid &&
                this.unsignedOid == that.unsignedOid &&
                this.typarray == that.typarray &&
                this.unsignedTyparray == that.unsignedTyparray &&
                Objects.equals(this.name, that.name) &&
                Objects.equals(this.category, that.category);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.oid, this.unsignedOid, this.typarray, this.unsignedTyparray, this.name, this.category);
        }

        @Override
        public String toString() {
            return "PostgresType{" +
                "oid=" + this.oid +
                "unsignedOid=" + this.unsignedOid +
                "typarray=" + this.typarray +
                "unsignedTyparray=" + this.unsignedTyparray +
                ", name='" + this.name + '\'' +
                ", category='" + this.category + '\'' +
                '}';
        }

    }

}
