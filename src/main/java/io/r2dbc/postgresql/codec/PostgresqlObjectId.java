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

import io.r2dbc.postgresql.api.RefCursor;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcTypes;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

/**
 * Object IDs for well know PostgreSQL data types.
 * <p>Extension Object IDs that are provided by Postgres extensions such as PostGIS are not constants of this enumeration and must be looked up from {@code pg_type}.
 * <p>
 * Lightweight {@link PostgresTypeIdentifier} identifier returning {@code Object.class} when calling {@link #getJavaType()}.
 */
public enum PostgresqlObjectId implements PostgresTypeIdentifier {

    /**
     * The bit object id.
     */
    BIT(1560),

    /**
     * The bit array object id.
     */
    BIT_ARRAY(1561),

    /**
     * The bool object id.
     */
    BOOL(16, Boolean.class),

    /**
     * The bool array object id.
     */
    BOOL_ARRAY(1000, Boolean[].class),

    /**
     * The box object id.
     */
    BOX(603, Box.class),

    /**
     * The bpchar object id.
     */
    BPCHAR(1042, String.class),

    /**
     * The bpchar array object id.
     */
    BPCHAR_ARRAY(1014, String[].class),

    /**
     * The bytea object id.
     */
    BYTEA(17, ByteBuffer.class),

    /**
     * They bytea array object id.
     */
    BYTEA_ARRAY(1001, ByteBuffer[].class),

    /**
     * The char object id.
     */
    CHAR(18, Character.class),

    /**
     * The char array object id.
     */
    CHAR_ARRAY(1002, Character[].class),

    /**
     * The circle object id
     */
    CIRCLE(718, Circle.class),

    /**
     * The date object id.
     */
    DATE(1082, LocalDate.class),

    /**
     * The date array object id.
     */
    DATE_ARRAY(1182, LocalDate[].class),

    /**
     * The float4 object id.
     */
    FLOAT4(700, Float.class),

    /**
     * The float4 array object id.
     */
    FLOAT4_ARRAY(1021, Float[].class),

    /**
     * The float8 object id.
     */
    FLOAT8(701, Double.class),

    /**
     * The float8 array object id.
     */
    FLOAT8_ARRAY(1022, Double[].class),

    /**
     * The inet object id.
     */
    INET(869, InetAddress.class),

    /**
     * The inet array object id.
     */
    INET_ARRAY(1041, InetAddress[].class),

    /**
     * The int2 object id.
     */
    INT2(21, Short.class),

    /**
     * The int2 array object id.
     */
    INT2_ARRAY(1005, Short[].class),

    /**
     * The int4 object id.
     */
    INT4(23, Integer.class),

    /**
     * The int4 array object id.
     */
    INT4_ARRAY(1007, Integer[].class),

    /**
     * The int8 object id.
     */
    INT8(20, Long.class),

    /**
     * The int8 array object id.
     */
    INT8_ARRAY(1016, Long[].class),

    /**
     * The interval object id.
     */
    INTERVAL(1186, Interval.class),

    /**
     * The interval array object id.
     */
    INTERVAL_ARRAY(1187, Interval[].class),

    /**
     * The JSON object id.
     */
    JSON(114, Json.class),

    /**
     * The JSON array object id.
     */
    JSON_ARRAY(199, Json[].class),

    /**
     * The JSONB array object id.
     */
    JSONB(3802, Json.class),

    /**
     * The JSONB array object id.
     */
    JSONB_ARRAY(3807, Json.class),

    /**
     * The line object id
     */
    LINE(628, Line.class),

    /**
     * The line segment object id
     */
    LSEG(601, Lseg.class),

    /**
     * The money object id.
     */
    MONEY(790),

    /**
     * The money array object id.
     */
    MONEY_ARRAY(791),

    /**
     * The name object id.
     */
    NAME(19, String.class),

    /**
     * The name array object id.
     */
    NAME_ARRAY(1003, String[].class),

    /**
     * The numberic object id.
     */
    NUMERIC(1700, BigDecimal.class),

    /**
     * The numeric array object id.
     */
    NUMERIC_ARRAY(1231, BigDecimal[].class),

    /**
     * The oid object id.
     */
    OID(26, Integer.class),

    /**
     * The oid array object id.
     */
    OID_ARRAY(1028, Integer[].class),

    /**
     * the path object id
     */
    PATH(602, Path.class),

    /**
     * The point object id.
     */
    POINT(600, Point.class),

    /**
     * The point array object id.
     */
    POINT_ARRAY(1017, Point[].class),

    /**
     * the polygon object id
     */
    POLYGON(604, Polygon.class),

    /**
     * The ref cursor object id.
     */
    REF_CURSOR(1790, RefCursor.class),

    /**
     * The ref cursor array object id.
     */
    REF_CURSOR_ARRAY(2201, RefCursor[].class),

    /**
     * The text object id.
     */
    TEXT(25, Clob.class),

    /**
     * The text array object id.
     */
    TEXT_ARRAY(1009, Clob[].class),

    /**
     * The time object id.
     */
    TIME(1083, LocalTime.class),

    /**
     * The time array object id.
     */
    TIME_ARRAY(1183, LocalTime[].class),

    /**
     * The timestamp object id.
     */
    TIMESTAMP(1114, LocalDateTime.class),

    /**
     * The timestamp array object id.
     */
    TIMESTAMP_ARRAY(1115, LocalDateTime[].class),

    /**
     * The timestamptz object id.
     */
    TIMESTAMPTZ(1184, OffsetDateTime.class),

    /**
     * The timestamptz array object id.
     */
    TIMESTAMPTZ_ARRAY(1185, OffsetDateTime[].class),

    /**
     * The timetz object id.
     */
    TIMETZ(1266, OffsetTime.class),

    /**
     * The timetz array object id.
     */
    TIMETZ_ARRAY(1270, OffsetTime[].class),

    /**
     * UNKNOWN type
     * PostgreSQL will sometimes return this type
     * an example might be select 'hello' as foo
     * newer versions return TEXT but some older
     * versions will return UNKNOWN
     */
    UNKNOWN(705),

    /**
     * The unspecified object id.
     * This can be sent as a parameter type
     * to tell the backend to infer the type
     */
    UNSPECIFIED(0),

    /**
     * The UUID object id.
     */
    UUID(2950, java.util.UUID.class),

    /**
     * The UUID array object id.
     */
    UUID_ARRAY(2951, java.util.UUID[].class),

    /**
     * The varbit object id.
     */
    VARBIT(1562),

    /**
     * The varbit array object id.
     */
    VARBIT_ARRAY(1563),

    /**
     * The varchar object id.
     */
    VARCHAR(1043, String.class),

    /**
     * The varchar array object id.
     */
    VARCHAR_ARRAY(1015, String[].class),

    /**
     * The void object id.
     */
    VOID(2278, Void.class),

    /**
     * The XML object id.
     */
    XML(142),

    /**
     * The XML Array object id.
     */
    XML_ARRAY(143);

    public static final int OID_CACHE_SIZE = 4096;

    private static final PostgresqlObjectId[] CACHE = new PostgresqlObjectId[OID_CACHE_SIZE];

    private final int objectId;

    private final Class<?> defaultJavaType;

    static {
        for (PostgresqlObjectId oid : values()) {
            CACHE[oid.getObjectId()] = oid;
        }
    }

    PostgresqlObjectId(int objectId) {
        this(objectId, Object.class);
    }

    PostgresqlObjectId(int objectId, Class<?> defaultJavaType) {
        this.objectId = objectId;
        this.defaultJavaType = Assert.requireNonNull(defaultJavaType, "defaultJavaType must not be null");
    }

    /**
     * Returns if the {@code objectId} is a known and valid {@code objectId}.
     *
     * @param objectId the object id to match
     * @return {@code true} if the {@code objectId} is a valid and known (static) objectId;{@code false} otherwise.
     */
    public static boolean isValid(int objectId) {

        if (objectId >= 0 && objectId < OID_CACHE_SIZE) {
            PostgresqlObjectId oid = CACHE[objectId];
            return oid != null;
        }

        try {
            valueOf(objectId);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns the {@link PostgresqlObjectId} matching a given object id.
     *
     * @param objectId the object id to match
     * @return the {@link PostgresqlObjectId} matching a given object id
     * @throws IllegalArgumentException if {@code objectId} isn't a valid object id
     */
    public static PostgresqlObjectId valueOf(int objectId) {

        if (objectId >= 0 && objectId < OID_CACHE_SIZE) {
            PostgresqlObjectId oid = CACHE[objectId];

            if (oid == null) {
                throw new IllegalArgumentException(String.format("%d is not a valid object id", objectId));
            }

            return oid;
        }

        for (PostgresqlObjectId type : values()) {
            if (type.objectId == objectId) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format("%d is not a valid object id", objectId));
    }

    /**
     * Returns the {@link PostgresqlObjectId} matching a given {@link R2dbcTypes R2DBC type}.
     *
     * @param type the R2DBC type
     * @return the {@link PostgresqlObjectId}
     * @throws IllegalArgumentException      if {@code type} is {@code null}
     * @throws UnsupportedOperationException if the given {@code type} is not supported
     * @since 0.9
     */
    public static PostgresqlObjectId valueOf(R2dbcTypes type) {

        Assert.requireNonNull(type, "type must not be null");

        switch (type) {

            case NCHAR:
            case CHAR:
                return CHAR;
            case NVARCHAR:
            case VARCHAR:
                return VARCHAR;
            case CLOB:
            case NCLOB:
                return TEXT;
            case BOOLEAN:
                return BOOL;
            case BINARY:
            case VARBINARY:
            case BLOB:
                return BYTEA;
            case INTEGER:
                return INT4;
            case TINYINT:
                return BIT;
            case SMALLINT:
                return INT2;
            case BIGINT:
                return INT8;
            case NUMERIC:
            case DECIMAL:
                return NUMERIC;
            case FLOAT:
            case REAL:
                return FLOAT4;
            case DOUBLE:
                return FLOAT8;
            case DATE:
                return DATE;
            case TIME:
                return TIME;
            case TIME_WITH_TIME_ZONE:
                return TIMETZ;
            case TIMESTAMP:
                return TIMESTAMP;
            case TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMPTZ;
            case COLLECTION:
                throw new UnsupportedOperationException("Raw collection (without component type) is not supported");
        }

        throw new UnsupportedOperationException("Type " + type + " not supported");
    }

    @Override
    public Class<?> getJavaType() {
        return this.defaultJavaType;
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * Returns the object id represented by each return value.
     *
     * @return the object id represented by each return value
     */
    @Override
    public int getObjectId() {
        return this.objectId;
    }

}
