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
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class DataRowTest {

    @Test
    void constructorNoColumns() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DataRow((ByteBuf[]) null))
            .withMessage("columns must not be null");
    }

    @Test
    void decode() {
        assertThat(DataRow.class)
            .decoded(buffer -> buffer
                .writeShort(1)
                .writeInt(4)
                .writeInt(100))
            .isEqualTo(new DataRow(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void decodeNullColumn() {
        assertThat(DataRow.class)
            .decoded(buffer -> buffer
                .writeShort(1)
                .writeInt(-1))
            .isEqualTo(new DataRow(new ByteBuf[]{null}));
    }

}
