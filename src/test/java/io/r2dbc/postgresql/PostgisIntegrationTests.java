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

package io.r2dbc.postgresql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for PostGIS functionality.
 */
final class PostgisIntegrationTests extends AbstractIntegrationTests {

    @Override
    @BeforeEach
    void setUp() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        try {
            jdbcOperations.execute("CREATE EXTENSION postgis");
        } catch (DataAccessException e) {
            // ignore
        }

        try {
            jdbcOperations.queryForMap("SELECT postgis_full_version()");
        } catch (DataAccessException e) {
            assumeThat(e).isNull();
        }

        super.setUp();
    }

    @Test
    void shouldWriteAndReadPointWithSRID() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        jdbcOperations.execute("DROP TABLE IF EXISTS geo_test");
        jdbcOperations.execute("CREATE TABLE geo_test (geom geometry)");

        PackedCoordinateSequenceFactory csFactory = new PackedCoordinateSequenceFactory();

        CoordinateSequence cs = csFactory.create(1, 2);
        // initialize with a data signature where coords look like [1, 10, 100, ...]
        for (int i = 0; i < 1; i++) {
            for (int d = 0; d < 2; d++) {
                cs.setOrdinate(i, d, i + 1 * Math.pow(10, d + 1));
            }
        }
        Point point = new Point(cs, new GeometryFactory());
        point.setSRID(4210);

        this.connection.createStatement("INSERT INTO geo_test VALUES($1)").bind("$1", point).execute().flatMap(it -> it.getRowsUpdated()).then().as(StepVerifier::create).verifyComplete();

        this.connection.createStatement("SELECT * FROM geo_test").execute().flatMap(it -> it.map(row -> row.get("geom"))).as(StepVerifier::create).consumeNextWith(actual -> {
            assertThat(actual).isEqualTo(point);
            assertThat(((Point) actual).getSRID()).isEqualTo(point.getSRID());
        }).verifyComplete();
    }

}
