/*
 * Copyright 2017-2019 the original author or authors.
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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.util.Assert;

import java.util.Objects;

/**
 * The ReadyForQuery message.
 */
public final class ReadyForQuery implements BackendMessage {

    private final TransactionStatus transactionStatus;

    /**
     * Creates a new message.
     *
     * @param transactionStatus the current backend transaction status
     * @throws IllegalArgumentException if {@code transactionStatus} is {@code null}
     */
    public ReadyForQuery(TransactionStatus transactionStatus) {
        this.transactionStatus = Assert.requireNonNull(transactionStatus, "transactionStatus must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadyForQuery that = (ReadyForQuery) o;
        return this.transactionStatus == that.transactionStatus;
    }

    /**
     * Returns the current backend transaction status.
     *
     * @return the current backend transaction status
     */
    public TransactionStatus getTransactionStatus() {
        return this.transactionStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.transactionStatus);
    }

    @Override
    public String toString() {
        return "ReadyForQuery{" +
            "transactionStatus=" + this.transactionStatus +
            '}';
    }

    static ReadyForQuery decode(ByteBuf in) {
        Assert.requireNonNull(in, "in must not be null");

        return new ReadyForQuery(TransactionStatus.valueOf(in.readByte()));
    }

    /**
     * An enumeration of backend transaction statuses.
     */
    public enum TransactionStatus {

        /**
         * The failed transaction status, represented by the {@code E} byte.
         */
        ERROR('E'),

        /**
         * The idle transaction status, represented by the {@code I} byte.
         */
        IDLE('I'),

        /**
         * The transaction transaction status, represented by the {@code T} byte.
         */
        TRANSACTION('T');

        private final char discriminator;

        TransactionStatus(char discriminator) {
            this.discriminator = discriminator;
        }

        static TransactionStatus valueOf(byte b) {
            for (TransactionStatus transactionStatus : values()) {
                if (transactionStatus.discriminator == b) {
                    return transactionStatus;
                }
            }
            throw new IllegalArgumentException(String.format("%c is not a valid transaction status", b));
        }

    }

}
