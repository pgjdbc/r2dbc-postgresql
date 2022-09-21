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

import io.r2dbc.postgresql.AbstractIntegrationTests;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.spi.Parameters;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link StringCodec} usage with CITEXT and customization options.
 */
class StringCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void stringCodecShouldConsiderCIText() {

        SERVER.getJdbcOperations().execute("CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public");

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test (ci CITEXT, cs VARCHAR)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES('HeLlO', 'HeLlO')");

        this.connection.createStatement("SELECT ci FROM test WHERE ci = $1::citext")
            .bind("$1", "Hello")
            .execute()
            .flatMap(it -> it.map(r -> r.get("ci")))
            .as(StepVerifier::create)
            .expectNext("HeLlO")
            .verifyComplete();

        this.connection.createStatement("SELECT ci FROM test WHERE ci = $1")
            .bind("$1", Parameters.in(PostgresqlObjectId.UNSPECIFIED, "Hello"))
            .execute()
            .flatMap(it -> it.map(r -> r.get("ci")))
            .as(StepVerifier::create)
            .expectNext("HeLlO")
            .verifyComplete();

        this.connection.createStatement("SELECT cs::citext = $1 FROM test")
            .bind("$1", Parameters.in(PostgresqlObjectId.UNSPECIFIED, "Hello"))
            .execute()
            .flatMap(it -> it.map(r -> r.get(0)))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        this.connection.createStatement("SELECT cs::citext = $1 FROM test")
            .bind("$1", "Hello")
            .execute()
            .flatMap(it -> it.map(r -> r.get(0)))
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

    @Test
    void shouldApplyCustomizedCodec() {

        SERVER.getJdbcOperations().execute("CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public");

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test ( ci CITEXT, cs VARCHAR)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES('HELLO', 'HELLO')");

        PostgresqlConnectionFactory custom = getConnectionFactory(builder -> builder.codecRegistrar((connection1, allocator, registry) -> {
            registry.addFirst(new StringCodec(allocator, PostgresqlObjectId.UNSPECIFIED, PostgresqlObjectId.VARCHAR_ARRAY));
            return Mono.empty();
        }));

        PostgresqlConnection customizedConnection = custom.create().block();

        customizedConnection.createStatement("SELECT cs::citext = $1 FROM test")
            .bind("$1", "Hello")
            .execute()
            .flatMap(it -> it.map(r -> r.get(0)))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        customizedConnection.close().block();

        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

}
