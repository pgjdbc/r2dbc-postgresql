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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.api.PostgresqlResult;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class NameArrayIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldReadArrayOfName() {

        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
        jdbcOperations.execute("DROP TABLE IF EXISTS name_table;");
        jdbcOperations.execute("CREATE TABLE name_table (n name)");

        this.connection.createStatement("INSERT INTO name_table (n) VALUES('hello'), ('world'), (null)")
            .execute()
            .flatMap(PostgresqlResult::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        this.connection.createStatement("select array_agg(n) as names from name_table")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("names")))
            .as(StepVerifier::create)
            .assertNext(arr -> {
                assertThat((Object[]) arr).containsExactly(new Object[]{"hello", "world", null});
            })
            .verifyComplete();
    }

}
