/*
 * Copyright 2024 original author or authors.
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
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link PrimitiveCodec} usage.
 */
class PrimitiveCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldReturnValues() {

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test (the_int INT2, the_bool BOOL)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES(1, true)");

        this.connection.createStatement("SELECT the_int FROM test")
            .execute()
            .flatMap(it -> it.map(r -> r.get("the_int", int.class)))
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        this.connection.createStatement("SELECT the_bool FROM test")
            .execute()
            .flatMap(it -> it.map(r -> r.get("the_bool", boolean.class)))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

    @Test
    void nullRetrievalShouldFail() {

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test (the_int INT2, the_bool BOOL)");
        SERVER.getJdbcOperations().execute("INSERT INTO test VALUES(null, null)");

        this.connection.createStatement("SELECT the_int FROM test")
            .execute()
            .flatMap(it -> it.map(r -> r.get("the_int", int.class)))
            .as(StepVerifier::create)
            .verifyError(NullPointerException.class);

        this.connection.createStatement("SELECT the_int FROM test")
            .execute()
            .flatMap(it -> it.map(r -> r.get(0, int.class)))
            .as(StepVerifier::create)
            .verifyError(NullPointerException.class);

        this.connection.createStatement("SELECT the_bool FROM test")
            .execute()
            .flatMap(it -> it.map(r -> r.get("the_bool", boolean.class)))
            .as(StepVerifier::create)
            .verifyError(NullPointerException.class);

        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

}
