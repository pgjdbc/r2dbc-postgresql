/*
 * Copyright 2017-2020 the original author or authors.
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

package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link TransactionStatus}.
 */
final class TransactionStatusUnitTests {

    @Test
    void valueOf() {
        assertThat(TransactionStatus.valueOf(ReadyForQuery.TransactionStatus.IDLE)).isEqualTo(TransactionStatus.IDLE);
    }

    @Test
    void valueOfNoT() {
        assertThatIllegalArgumentException().isThrownBy(() -> TransactionStatus.valueOf((ReadyForQuery.TransactionStatus) null))
            .withMessage("t must not be null");
    }

}
