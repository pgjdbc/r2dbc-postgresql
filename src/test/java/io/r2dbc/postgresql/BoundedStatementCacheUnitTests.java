/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.CompositeFrontendMessage;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Sync;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link BoundedStatementCache}.
 */
class BoundedStatementCacheUnitTests {

    @Test
    void name() {

        BoundedStatementCache cache = new BoundedStatementCache(mock(Client.class), 100);

        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_0");
        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_1");

        cache.put(new Binding(0), "foo", "S_0");
        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_0");

        assertThat(cache.getName(new Binding(0), "bar")).isEqualTo("S_2");
    }

    @Test
    void requiresPrepare() {

        BoundedStatementCache cache = new BoundedStatementCache(mock(Client.class), 100);

        assertThat(cache.requiresPrepare(new Binding(0), "foo")).isTrue();
        cache.put(new Binding(0), "foo", "S_0");
        assertThat(cache.requiresPrepare(new Binding(0), "foo")).isFalse();
    }

    @Test
    void put() {

        TestClient client = TestClient.builder().expectRequest(new CompositeFrontendMessage(new Close("S_0", ExecutionType.STATEMENT), Sync.INSTANCE)).thenRespond(CloseComplete.INSTANCE).build();

        BoundedStatementCache cache = new BoundedStatementCache(client, 2);

        cache.put(new Binding(0), "foo", "S_0");
        assertThat(cache.getCachedStatementNames()).containsOnly("S_0");

        cache.put(new Binding(0), "bar", "S_1");
        assertThat(cache.getCachedStatementNames()).containsOnly("S_0", "S_1");

        cache.put(new Binding(0), "baz", "S_2");
        assertThat(cache.getCachedStatementNames()).containsOnly("S_1", "S_2");
    }

    @Test
    void putCloseFails() {

        TestClient client =
            TestClient.builder().expectRequest(new CompositeFrontendMessage(new Close("S_0", ExecutionType.STATEMENT), Sync.INSTANCE)).thenRespond(new ErrorResponse(Collections.emptyList())).build();

        BoundedStatementCache cache = new BoundedStatementCache(client, 2);

        cache.put(new Binding(0), "foo", "S_0");
        cache.put(new Binding(0), "bar", "S_1");

        cache.put(new Binding(0), "baz", "S_2");
        assertThat(cache.getCachedStatementNames()).containsOnly("S_1", "S_2");
    }

}
