/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.codec.Vector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for {@link Vector}.
 */
final class VectorIntegrationTests extends AbstractIntegrationTests {

    @Override
    @BeforeEach
    void setUp() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        List<Map<String, Object>> extensions = jdbcOperations.queryForList("select * from pg_available_extensions() where name = 'vector'");
        assumeThat(extensions).isNotEmpty();

        jdbcOperations.execute("CREATE EXTENSION IF NOT EXISTS VECTOR;");
        jdbcOperations.execute("CREATE TABLE IF NOT EXISTS vector_arrays (id bigserial PRIMARY KEY, embedding vector[3]);");
        jdbcOperations.execute("CREATE TABLE IF NOT EXISTS vector_items (id bigserial PRIMARY KEY, embedding vector(3));");

        jdbcOperations.execute("DELETE FROM vector_arrays;");
        jdbcOperations.execute("DELETE FROM vector_items;");

        super.setUp();
    }

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.forceBinary(true);
        super.customize(builder);
    }

    @Test
    void shouldReadVector() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        jdbcOperations.execute("INSERT INTO vector_items (embedding) VALUES ('[1,2,3]'), ('[4,5,6]');");

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("SELECT * FROM vector_items WHERE id != $1 ORDER BY embedding <-> '[3,1,2]' ")
            .bind("$1", 1)
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding")))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector.class).isEqualTo(Vector.of(1, 2, 3));
            })
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector.class).isEqualTo(Vector.of(4, 5, 6));
            })
            .verifyComplete();

        connection.createStatement("SELECT * FROM vector_items WHERE id != $1 ORDER BY embedding <-> '[3,1,2]' ")
            .bind("$1", 1)
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding", float[].class)))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).contains(1f, 2f, 3f);
            })
            .assertNext(o -> {
                assertThat(o).contains(4, 5, 6);
            })
            .verifyComplete();

        connection.close().block();
    }

    @Test
    void shouldReadVectorArray() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        jdbcOperations.execute("INSERT INTO vector_arrays (embedding) VALUES (ARRAY['[1,2,3]'::vector,'[4,5,6]'::vector]);");

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("SELECT * FROM vector_arrays WHERE id != $1")
            .bind("$1", 1)
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding")))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector[].class);
                assertThat((Vector[]) o).contains(Vector.of(1, 2, 3), Vector.of(4, 5, 6));
            })
            .verifyComplete();

        connection.close().block();
    }

    @Test
    void shouldWriteVector() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("INSERT INTO vector_items (embedding) VALUES ($1)")
            .bind("$1", Vector.of(1, 2, 3))
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT * FROM vector_items")
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding")))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector.class).isEqualTo(Vector.of(1, 2, 3));
            }).verifyComplete();

        connection.close().block();
    }

    @Test
    void shouldWriteVectorArray() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("INSERT INTO vector_arrays (embedding) VALUES ($1);")
            .bind("$1", new Vector[]{Vector.of(1, 2, 3), Vector.of(4, 5, 6)})
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT * FROM vector_arrays WHERE id != $1")
            .bind("$1", 1)
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding")))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector[].class);
                assertThat((Vector[]) o).contains(Vector.of(1, 2, 3), Vector.of(4, 5, 6));
            })
            .verifyComplete();

        connection.close().block();
    }

    @Test
    void shouldWriteVectorArrayWithNulls() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.createStatement("INSERT INTO vector_arrays (embedding) VALUES ($1);")
            .bind("$1", new Vector[]{Vector.of(1, 2, 3), null, Vector.of(4, 5, 6)})
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createStatement("SELECT * FROM vector_arrays WHERE id != $1")
            .bind("$1", 1)
            .execute()
            .flatMap(result -> result.map(readable -> readable.get("embedding")))
            .as(StepVerifier::create)
            .assertNext(o -> {
                assertThat(o).isInstanceOf(Vector[].class);
                assertThat((Vector[]) o).contains(Vector.of(1, 2, 3), Vector.of(4, 5, 6));
            })
            .verifyComplete();

        connection.close().block();
    }

}
