/*
 * Copyright 2026 the original author or authors.
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

package io.r2dbc.postgresql.util;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * Opt-in BlockHound integration for PostgreSQL connection startup.
 * <p>
 * Register explicitly with {@code BlockHound.builder().loadIntegrations()
 * .with(new PostgresqlBlockHoundIntegration()).install()}.
 * The two allowances apply to all blocking calls nested in those operations. They preserve
 * the driver's TLS and SCRAM settings and do not move the operations off the event loop.
 */
public final class PostgresqlBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        builder.allowBlockingCallsInside("io.r2dbc.postgresql.PostgresqlConnectionConfiguration$Builder", "buildSslContext");
        builder.allowBlockingCallsInside("io.r2dbc.postgresql.authentication.SASLAuthenticationHandler", "buildScramClient");
    }

}
