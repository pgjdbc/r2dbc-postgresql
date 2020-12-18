/*
 * Copyright 2017 the original author or authors.
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

interface StatementCache {

    String getName(Binding binding, String sql);

    boolean requiresPrepare(Binding binding, String sql);

    void put(Binding binding, String sql, String name);

    static StatementCache fromPreparedStatementCacheQueries(Client client, int preparedStatementCacheQueries) {
        if (preparedStatementCacheQueries < 0) {
            return new IndefiniteStatementCache();
        }
        if (preparedStatementCacheQueries == 0) {
            return new DisabledStatementCache();
        }
        return new BoundedStatementCache(client, preparedStatementCacheQueries);
    }

}
