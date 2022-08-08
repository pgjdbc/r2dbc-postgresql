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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * @author Mark Paluch
 */
public class Repro extends AbstractIntegrationTests{

    @Test
    void name() {


        SERVER.getJdbcOperations().execute("DROP TABLE IF EXISTS repro;");
        SERVER.getJdbcOperations().execute("CREATE TABLE repro (    created_at timestamp not null,\n" +
            "    mytime time not null,\n" +
            "    updated_at timestamp not null,\n" +
            "    transaction_amount bigint not null,\n" +
            "    transaction_currency varchar(3) not null,\n" +
            "    transaction_at timestamptz not null)");

        for (int i = 0; i < 10; i++) {

            connection.createStatement("INSERT INTO repro VALUES($1, $2, $3, $4, $5, $6)")
            .bind("$1", Instant.now())
            .bind("$2", LocalTime.now())
            .bind("$3", Instant.now())
            .bind("$4", 1234)
            .bind("$5", "FOO")
            .bind("$6", OffsetDateTime.now()).execute().flatMap(it -> it.getRowsUpdated()).blockLast();
        }

    }

}
