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

import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
class SimpleTransactionDefinition implements PostgresTransactionDefinition {

    public static final SimpleTransactionDefinition EMPTY = new SimpleTransactionDefinition(Collections.emptyMap());

    private final Map<Option<?>, Object> options;

    SimpleTransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @Override
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    public PostgresTransactionDefinition with(Option<?> option, Object value) {

        Map<Option<?>, Object> options = new HashMap<>(this.options);
        options.put(Assert.requireNonNull(option, "option must not be null"), Assert.requireNonNull(value, "value must not be null"));

        return new SimpleTransactionDefinition(options);
    }

    @Override
    public PostgresTransactionDefinition readOnly() {
        return with(PostgresTransactionDefinition.READ_ONLY, true);
    }

    @Override
    public PostgresTransactionDefinition readWrite() {
        return with(PostgresTransactionDefinition.READ_ONLY, false);
    }

    @Override
    public PostgresTransactionDefinition isolationLevel(IsolationLevel isolationLevel) {
        return with(PostgresTransactionDefinition.ISOLATION_LEVEL, isolationLevel);
    }

    @Override
    public PostgresTransactionDefinition deferrable() {
        return with(PostgresTransactionDefinition.DEFERRABLE, true);
    }

    @Override
    public PostgresTransactionDefinition notDeferrable() {
        return with(PostgresTransactionDefinition.DEFERRABLE, false);
    }

}
