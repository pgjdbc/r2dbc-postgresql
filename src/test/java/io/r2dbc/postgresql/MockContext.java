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
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.PortalNameSupplier;
import io.r2dbc.postgresql.codec.Codecs;
import io.r2dbc.postgresql.codec.MockCodecs;

import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;

final class MockContext {

    public static MockContext.Builder builder() {
        return new MockContext.Builder();
    }

    public static ConnectionResources empty() {
        return builder().build();
    }

    public static final class Builder {

        private Codecs codecs = MockCodecs.empty();

        private Client client;

        private PostgresqlConnection connection;

        private StatementCache statementCache = mock(StatementCache.class, RETURNS_SMART_NULLS);

        private boolean forceBinary = false;

        private PortalNameSupplier portalNameSupplier = () -> "";

        private Builder() {
        }

        public ConnectionResources build() {
            PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
                .host("localhost")
                .username("admin")
                .password("password")
                .forceBinary(forceBinary)
                .build();
            return new ConnectionResources(this.client, this.codecs, this.connection, configuration, portalNameSupplier, statementCache);
        }

        public Builder codecs(Codecs codecs) {
            this.codecs = codecs;
            return this;
        }

        public Builder client(Client client) {
            this.client = client;
            return this;
        }

        public Builder connection(PostgresqlConnection connection) {
            this.connection = connection;
            return this;
        }

        public Builder statementCache(StatementCache statementCache) {
            this.statementCache = statementCache;
            return this;
        }

        public Builder forceBinary(boolean forceBinary) {
            this.forceBinary = forceBinary;
            return this;
        }

        public Builder portalNameSupplier(PortalNameSupplier portalNameSupplier) {
            this.portalNameSupplier = portalNameSupplier;
            return this;
        }

    }

}
