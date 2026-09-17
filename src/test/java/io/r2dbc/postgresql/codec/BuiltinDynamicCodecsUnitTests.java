/*
 * Copyright 2026 the original author or authors.
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

import io.r2dbc.postgresql.client.ParameterAssert;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.r2dbc.postgresql.codec.BuiltinDynamicCodecs.BuiltinCodec.POSTGIS_GEOGRAPHY;
import static io.r2dbc.postgresql.codec.BuiltinDynamicCodecs.BuiltinCodec.POSTGIS_GEOMETRY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BuiltinDynamicCodecs}, in particular the pairing of the {@code geometry} and {@code geography} OIDs into a single {@link PostgisCodec}.
 */
final class BuiltinDynamicCodecsUnitTests {

    private static final int GEOMETRY_OID = 111111;

    private static final int GEOGRAPHY_OID = 222222;

    private final BuiltinDynamicCodecs codecs = new BuiltinDynamicCodecs();

    private final GeometryFactory geometryFactory = new GeometryFactory();

    @Test
    void registersSinglePostgisCodecForGeometryAndGeography() {

        List<BuiltinDynamicCodecs.DiscoveredType> discovered = Arrays.asList(
            new BuiltinDynamicCodecs.DiscoveredType(POSTGIS_GEOMETRY, GEOMETRY_OID, PostgresTypes.NO_SUCH_TYPE),
            new BuiltinDynamicCodecs.DiscoveredType(POSTGIS_GEOGRAPHY, GEOGRAPHY_OID, PostgresTypes.NO_SUCH_TYPE)
        );

        DefaultCodecs registry = new DefaultCodecs(TEST);
        this.codecs.registerCodecs(discovered, TEST, registry);

        List<PostgisCodec> postgisCodecs = findPostgisCodecs(registry);
        assertThat(postgisCodecs).hasSize(1);

        PostgisCodec codec = postgisCodecs.get(0);
        assertThat(codec.canDecode(GEOMETRY_OID, FORMAT_TEXT, Geometry.class)).isTrue();
        assertThat(codec.canDecode(GEOGRAPHY_OID, FORMAT_TEXT, Geometry.class)).isTrue();

        ParameterAssert.assertThat(codec.encode(this.geometryFactory.createPoint())).hasType(GEOMETRY_OID);
    }

    @Test
    void registersPostgisCodecForGeometryOnlyWhenGeographyMissing() {

        List<BuiltinDynamicCodecs.DiscoveredType> discovered = Collections.singletonList(
            new BuiltinDynamicCodecs.DiscoveredType(POSTGIS_GEOMETRY, GEOMETRY_OID, PostgresTypes.NO_SUCH_TYPE)
        );

        DefaultCodecs registry = new DefaultCodecs(TEST);
        this.codecs.registerCodecs(discovered, TEST, registry);

        List<PostgisCodec> postgisCodecs = findPostgisCodecs(registry);
        assertThat(postgisCodecs).hasSize(1);

        PostgisCodec codec = postgisCodecs.get(0);
        assertThat(codec.canDecode(GEOMETRY_OID, FORMAT_TEXT, Geometry.class)).isTrue();
        assertThat(codec.canDecode(GEOGRAPHY_OID, FORMAT_TEXT, Geometry.class)).isFalse();
    }

    @Test
    void registersNoPostgisCodecWhenGeometryMissing() {

        List<BuiltinDynamicCodecs.DiscoveredType> discovered = Collections.singletonList(
            new BuiltinDynamicCodecs.DiscoveredType(POSTGIS_GEOGRAPHY, GEOGRAPHY_OID, PostgresTypes.NO_SUCH_TYPE)
        );

        DefaultCodecs registry = new DefaultCodecs(TEST);
        this.codecs.registerCodecs(discovered, TEST, registry);

        assertThat(findPostgisCodecs(registry)).isEmpty();
    }

    private static List<PostgisCodec> findPostgisCodecs(DefaultCodecs registry) {

        List<PostgisCodec> result = new ArrayList<>();
        for (Codec<?> codec : registry) {
            if (codec instanceof PostgisCodec) {
                result.add((PostgisCodec) codec);
            }
        }
        return result;
    }

}
