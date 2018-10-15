/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.backend.ReadyForQuery;

import java.util.Objects;

/**
 * An enumeration of transaction statuses.
 */
public enum TransactionStatus {

    /**
     * Indicates that the open transaction has failed.
     */
    FAILED(ReadyForQuery.TransactionStatus.ERROR),

    /**
     * Indicates that there is no open transaction.
     */
    IDLE(ReadyForQuery.TransactionStatus.IDLE),

    /**
     * Indicates that there is an open transaction.
     */
    OPEN(ReadyForQuery.TransactionStatus.TRANSACTION);

    private final ReadyForQuery.TransactionStatus discriminator;

    TransactionStatus(ReadyForQuery.TransactionStatus discriminator) {
        this.discriminator = Objects.requireNonNull(discriminator, "discriminator must not be null");
    }

    static TransactionStatus valueOf(ReadyForQuery.TransactionStatus t) {
        Objects.requireNonNull(t, "t must not be null");

        for ( TransactionStatus transactionStatus : values() ) {
            if (transactionStatus.discriminator == t ) {
                return transactionStatus;
            }
        }
        throw new IllegalArgumentException(String.format("%s is not a valid transaction status", t));
    }

}
