/*
 * Copyright 2022 the original author or authors.
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
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.ParameterAssert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKBWriter;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.JSON;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.JSONB;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link PostgisGeometryCodec}.
 */
final class PostgisGeometryCodecUnitTests {

    private static final int WGS84_SRID = 4326;

    private static final int dataType = 23456;

    private final PostgisGeometryCodec codec = new PostgisGeometryCodec(dataType);

    private final WKBWriter wkbWriter = new WKBWriter();

    private final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), WGS84_SRID);

    private final Point point = this.geometryFactory.createPoint(new Coordinate(1.0, 1.0));

    @Test
    void canDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.canDecode(dataType, null, Geometry.class))
            .withMessage("format must not be null");
    }

    @Test
    void canDecodeNoClass() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.canDecode(dataType, FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void canDecode() {
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, Geometry.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_BINARY, Geometry.class)).isTrue();

        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, Point.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, MultiPoint.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, LineString.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, LinearRing.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, MultiLineString.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, Polygon.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, MultiPolygon.class)).isTrue();
        assertThat(this.codec.canDecode(dataType, FORMAT_TEXT, GeometryCollection.class)).isTrue();

        assertThat(this.codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, Geometry.class)).isFalse();
        assertThat(this.codec.canDecode(JSON.getObjectId(), FORMAT_TEXT, Geometry.class)).isFalse();
        assertThat(this.codec.canDecode(JSONB.getObjectId(), FORMAT_BINARY, Geometry.class)).isFalse();
    }

    @Test
    void canEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.canEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void canEncode() {
        assertThat(this.codec.canEncode(this.geometryFactory.createPoint())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createMultiPoint())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createLineString())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createLinearRing())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createMultiLineString())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createPolygon())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createMultiPolygon())).isTrue();
        assertThat(this.codec.canEncode(this.geometryFactory.createGeometryCollection())).isTrue();

        assertThat(this.codec.canEncode("Geometry")).isFalse();
        assertThat(this.codec.canEncode(1)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void decode() {
        byte[] pointBytes = this.wkbWriter.write(this.point);
        ByteBuf pointByteBuf = ByteBufUtils.encode(TEST, WKBWriter.toHex(pointBytes));

        assertThat(this.codec.decode(pointByteBuf, dataType, FORMAT_TEXT, Geometry.class)).isEqualTo(this.point);
    }

    @Test
    @SuppressWarnings("unchecked")
    void decodeNoByteBuf() {
        assertThat(this.codec.decode(null, dataType, FORMAT_TEXT, Geometry.class)).isNull();
    }

    @Test
    void encode() {
        ByteBuf encoded = Unpooled.wrappedBuffer(new WKBWriter(2, true).write(this.point));

        ParameterAssert.assertThat(this.codec.encode(this.point))
            .hasFormat(FORMAT_BINARY)
            .hasType(dataType)
            .hasValue(encoded);
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.codec.encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new PostgisGeometryCodec(dataType).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
