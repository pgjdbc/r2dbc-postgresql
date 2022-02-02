package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
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
 * Unit tests for {@link PostgisGeometryCodec }.
 */
final class PostgisGeometryCodecUnitTests {

    private static final int WGS84_SRID = 4326;

    private static final int dataType = 23456;

    private final PostgisGeometryCodec codec = new PostgisGeometryCodec(TEST, dataType);

    private final WKBWriter wkbWriter = new WKBWriter();

    private final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), WGS84_SRID);

    private final Point point = geometryFactory.createPoint(new Coordinate(1.0, 1.0));

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgisGeometryCodec(null, dataType))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void canDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canDecode(dataType, null, Geometry.class))
            .withMessage("format must not be null");
    }

    @Test
    void canDecodeNoClass() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canDecode(dataType, FORMAT_TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void canDecode() {
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Geometry.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_BINARY, Geometry.class)).isTrue();

        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Point.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, MultiPoint.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, LineString.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, LinearRing.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, MultiLineString.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, Polygon.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, MultiPolygon.class)).isTrue();
        assertThat(codec.canDecode(dataType, FORMAT_TEXT, GeometryCollection.class)).isTrue();

        assertThat(codec.canDecode(VARCHAR.getObjectId(), FORMAT_BINARY, Geometry.class)).isFalse();
        assertThat(codec.canDecode(JSON.getObjectId(), FORMAT_TEXT, Geometry.class)).isFalse();
        assertThat(codec.canDecode(JSONB.getObjectId(), FORMAT_BINARY, Geometry.class)).isFalse();
    }

    @Test
    void canEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.canEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void canEncode() {
        assertThat(codec.canEncode(geometryFactory.createPoint())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createMultiPoint())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createLineString())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createLinearRing())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createMultiLineString())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createPolygon())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createMultiPolygon())).isTrue();
        assertThat(codec.canEncode(geometryFactory.createGeometryCollection())).isTrue();

        assertThat(codec.canEncode("Geometry")).isFalse();
        assertThat(codec.canEncode(1)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void decode() {
        byte[] pointBytes = wkbWriter.write(point);
        ByteBuf pointByteBuf = ByteBufUtils.encode(TEST, WKBWriter.toHex(pointBytes));

        assertThat(codec.decode(pointByteBuf, dataType, FORMAT_TEXT, Geometry.class)).isEqualTo(point);
    }

    @Test
    @SuppressWarnings("unchecked")
    void decodeNoByteBuf() {
        assertThat(codec.decode(null, dataType, FORMAT_TEXT, Geometry.class)).isNull();
    }

    @Test
    void encode() {
        ByteBuf encoded = ByteBufUtils.encode(TEST, point.toText());

        ParameterAssert.assertThat(codec.encode(point))
            .hasFormat(FORMAT_TEXT)
            .hasType(dataType)
            .hasValue(encoded);
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> codec.encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new PostgisGeometryCodec(TEST, dataType).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_BINARY, dataType, NULL_VALUE));
    }

}
