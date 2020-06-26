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

import io.r2dbc.postgresql.AbstractIntegrationTests;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link EnumCodec}.
 */
final class EnumCodecIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldReportUnresolvableTypes() {

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .database(SERVER.getDatabase())
            .host(SERVER.getHost())
            .port(SERVER.getPort())
            .password(SERVER.getPassword())
            .username(SERVER.getUsername())
            .codecRegistrar(EnumCodec.builder().withEnum("do_not_exist", MyEnum.class).build())
            .build();

        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(configuration);
        connectionFactory.create().flatMap(PostgresqlConnection::close).as(StepVerifier::create).verifyComplete();

        // we cannot really assert logs so that's up to you.
    }

    enum MyEnum {
        HELLO;
    }

}
