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

package io.r2dbc.postgresql.api;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

/**
 * {@link TransactionDefinition} for a PostgreSQL database.
 *
 * @since 0.9
 */
public interface PostgresTransactionDefinition extends TransactionDefinition {

    /**
     * The {@code DEFERRABLE} transaction_mode is a PostgreSQL language extension.
     */
    Option<Boolean> DEFERRABLE = Option.valueOf("deferrable");

    /**
     * Creates a {@link PostgresTransactionDefinition} given {@link IsolationLevel}.
     *
     * @param isolationLevel the isolation level to use during the transaction.
     * @return a new {@link PostgresTransactionDefinition} using {@link IsolationLevel}.
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}.
     */
    static PostgresTransactionDefinition from(IsolationLevel isolationLevel) {
        return SimpleTransactionDefinition.EMPTY.isolationLevel(isolationLevel);
    }

    /**
     * Creates a {@link PostgresTransactionDefinition} specifying transaction mutability.
     *
     * @param readWrite {@code true} for read-write transactions; {@code false} to use a read-only transaction.
     * @return a new {@link PostgresTransactionDefinition} using the specified transaction mutability.
     */
    static PostgresTransactionDefinition mutability(boolean readWrite) {
        return readWrite ? SimpleTransactionDefinition.EMPTY.readWrite() : SimpleTransactionDefinition.EMPTY.readOnly();
    }

    /**
     * Creates a {@link PostgresTransactionDefinition} retaining all configured options and using deferrable transaction semantics.
     * Overrides transaction deferrability if configured already.
     *
     * @return a new {@link PostgresTransactionDefinition} retaining all configured options and using deferrable transaction semantics.
     */
    PostgresTransactionDefinition deferrable();

    /**
     * Creates a {@link PostgresTransactionDefinition} retaining all configured options and using not deferrable transaction semantics.
     * Overrides transaction deferrability if configured already.
     *
     * @return a new {@link PostgresTransactionDefinition} retaining all configured options and using not deferrable transaction semantics.
     */
    PostgresTransactionDefinition notDeferrable();

    /**
     * Creates a {@link PostgresTransactionDefinition} retaining all configured options and applying {@link IsolationLevel}.
     *
     * @param isolationLevel the isolation level to use during the transaction.
     * @return a new {@link PostgresTransactionDefinition} retaining all configured options and applying {@link IsolationLevel}.
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}.
     */
    PostgresTransactionDefinition isolationLevel(IsolationLevel isolationLevel);

    /**
     * Creates a {@link PostgresTransactionDefinition} retaining all configured options and using read-only transaction semantics.
     * Overrides transaction muatbility if configured already.
     *
     * @return a new {@link PostgresTransactionDefinition} retaining all configured options and using read-only transaction semantics.
     */
    PostgresTransactionDefinition readOnly();

    /**
     * Creates a {@link PostgresTransactionDefinition} retaining all configured options and using read-write transaction semantics.
     * Overrides transaction mutability if configured already.
     *
     * @return a new {@link PostgresTransactionDefinition} retaining all configured options and using read-write transaction semantics.
     */
    PostgresTransactionDefinition readWrite();

}
