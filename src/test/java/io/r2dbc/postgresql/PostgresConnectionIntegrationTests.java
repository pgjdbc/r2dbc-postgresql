/*
 * Copyright 2021 the original author or authors.
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

import io.r2dbc.postgresql.api.PostgresTransactionDefinition;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link io.r2dbc.postgresql.PostgresqlConnection}.
 */
final class PostgresConnectionIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldStartExtendedTransaction() {

        this.connection.beginTransaction(PostgresTransactionDefinition.mutability(true).isolationLevel(IsolationLevel.READ_UNCOMMITTED).deferrable()).as(StepVerifier::create).verifyComplete();

        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read uncommitted").verifyComplete();
    }

    @Test
    void shouldApplyIsolationLevel() {

        this.connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
        this.connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read uncommitted").verifyComplete();

        this.connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read uncommitted").verifyComplete();
        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    void shouldApplyIsolationLevelInTx() {

        this.connection.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE).as(StepVerifier::create).verifyComplete();
        this.connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        this.connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read uncommitted").verifyComplete();

        this.connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("serializable").verifyComplete();
        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.SERIALIZABLE);
    }

    @Test
    void shouldRestoreIsolationLevelAfterTransaction() {

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read committed").verifyComplete();

        this.connection.beginTransaction(PostgresTransactionDefinition.mutability(true).isolationLevel(IsolationLevel.READ_UNCOMMITTED)).as(StepVerifier::create).verifyComplete();

        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_UNCOMMITTED);

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read uncommitted").verifyComplete();

        this.connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        this.connection.createStatement("SHOW TRANSACTION ISOLATION LEVEL")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> {
                return row.get(0, String.class);
            })).as(StepVerifier::create).expectNext("read committed").verifyComplete();

        assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(IsolationLevel.READ_COMMITTED);
    }

}
