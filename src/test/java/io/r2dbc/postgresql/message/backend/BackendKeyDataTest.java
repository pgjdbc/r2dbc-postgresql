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

import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;

final class BackendKeyDataTest {

    @Test
    void decode() {
        assertThat(BackendKeyData.class)
            .decoded(buffer -> buffer.writeInt(100).writeInt(200))
            .isEqualTo(new BackendKeyData(100, 200));
    }

}
