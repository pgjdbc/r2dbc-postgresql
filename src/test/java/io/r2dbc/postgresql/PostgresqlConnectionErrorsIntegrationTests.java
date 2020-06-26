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

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcRollbackException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for various error cases using {@link PostgresqlConnection}.
 */
final class PostgresqlConnectionErrorsIntegrationTests extends AbstractIntegrationTests {

    @Test
    void commitShouldRecoverFromFailedTransaction() {

        this.connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        this.connection.createStatement("error").execute().flatMap(PostgresqlResult::getRowsUpdated).as(StepVerifier::create).verifyError(R2dbcBadGrammarException.class);

        this.connection.commitTransaction().as(StepVerifier::create).verifyError(R2dbcRollbackException.class);

        Awaitility.await().until(() -> this.connection.isAutoCommit());
        assertThat(this.connection.isAutoCommit()).isTrue();
    }

    @Test
    void rollbackShouldRecoverFromFailedTransaction() {

        this.connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        this.connection.createStatement("error").execute().flatMap(PostgresqlResult::getRowsUpdated).as(StepVerifier::create).verifyError(R2dbcBadGrammarException.class);

        this.connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        assertThat(this.connection.isAutoCommit()).isTrue();
    }

}
