/*
 * Copyright 2019-2020 the original author or authors.
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

import com.zaxxer.hikari.HikariDataSource;
import io.r2dbc.postgresql.util.PgPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link ExtendedQueryPostgresqlStatement} using PgPool.
 */
final class PgPoolExtendedQueryPostgresqlStatementIntegrationTests extends ExtendedQueryPostgresqlStatementIntegrationTests {

    PgPool pgPool;

    HikariDataSource dataSource;

    JdbcOperations jdbcOperations;

    @BeforeEach
    void setUp() {
        this.pgPool = new PgPool(SERVER);

        this.dataSource = new HikariDataSource();
        this.dataSource.setUsername(SERVER.getUsername());
        this.dataSource.setPassword(SERVER.getPassword());
        this.dataSource.setJdbcUrl(String.format("jdbc:postgresql://%s:%d/%s", this.pgPool.getHost(), this.pgPool.getPort(), SERVER.getDatabase()));

        this.jdbcOperations = new JdbcTemplate(this.dataSource);

        super.setUp();
    }

    @AfterEach
    void tearDown() {
        super.tearDown();

        this.dataSource.close();
        this.pgPool.close();
    }

    @Override
    protected void customize(PostgresqlConnectionConfiguration.Builder builder) {
        builder.compatibilityMode(true);
        builder.host(this.pgPool.getHost()).port(this.pgPool.getPort());
    }

}
