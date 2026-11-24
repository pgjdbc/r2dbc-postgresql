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

import com.ongres.scram.client.ScramClient;
import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

import java.lang.reflect.Method;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class PostgresqlBlockHoundIntegrationUnitTests {

    @Test
    void registersOnlyConnectionStartupAllowances() throws NoSuchMethodException {
        BlockHound.Builder builder = mock(BlockHound.Builder.class);
        Method tls = PostgresqlConnectionConfiguration.Builder.class.getDeclaredMethod("buildSslContext", SslContextBuilder.class);
        Method scram = SASLAuthenticationHandler.class.getDeclaredMethod("buildScramClient", ScramClient.FinalBuildStage.class);

        new PostgresqlBlockHoundIntegration().applyTo(builder);

        verify(builder).allowBlockingCallsInside(tls.getDeclaringClass().getName(), tls.getName());
        verify(builder).allowBlockingCallsInside(scram.getDeclaringClass().getName(), scram.getName());
        verifyNoMoreInteractions(builder);
    }

    @Test
    void requiresExplicitRegistration() {
        for (BlockHoundIntegration integration : ServiceLoader.load(BlockHoundIntegration.class)) {
            assertFalse(integration instanceof PostgresqlBlockHoundIntegration);
        }
    }

}
