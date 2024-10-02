/*
 * Copyright 2024 the original author or authors.
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
 * Integration tests for {@link ObjectCodec} usage.
 */
class ObjectCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldEncodeNull() {

        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS test");
        SERVER.getJdbcOperations().execute("CREATE TABLE test (ci INT4, cs VARCHAR)");

        this.connection.createStatement("INSERT INTO test VALUES($1, $2)")
            .bindNull("$1", Object.class)
            .bindNull("$2", Object.class)
            .execute()
            .flatMap(it -> it.getRowsUpdated())
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");
    }

}
