/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import com.ongres.scram.client.ScramClient;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.TestClient;
import io.r2dbc.postgresql.message.backend.AuthenticationMD5Password;
import io.r2dbc.postgresql.message.backend.AuthenticationOk;
import io.r2dbc.postgresql.message.backend.AuthenticationSASL;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.frontend.PasswordMessage;
import io.r2dbc.postgresql.message.frontend.SASLInitialResponse;
import io.r2dbc.postgresql.message.frontend.StartupMessage;
import io.r2dbc.postgresql.util.ByteBufferUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static com.ongres.scram.client.ScramClient.ChannelBinding.NO;
import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class PostgresqlConnectionFactoryTest {

    @Test
    void constructorNoClientFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlConnectionFactory(null, PostgresqlConnectionConfiguration.builder()
            .host("test-host")
            .password("test-password")
            .username("test-username")
            .build()))
            .withMessage("clientFactory must not be null");
    }

    @Test
    void constructorNoConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PostgresqlConnectionFactory(null))
            .withMessage("configuration must not be null");
    }

    @Test
    void createAuthenticationMD5Password() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))
                .expectRequest(new PasswordMessage("md55e9836cdb369d50e3bc7d127e88b4804")).thenRespond(AuthenticationOk.INSTANCE)
                .done()
            .build();
        // @formatter:on

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .database("test-database")
            .host("test-host")
            .username("test-username")
            .password("test-password")
            .build();

        new PostgresqlConnectionFactory(Mono.just(client), configuration)
            .create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void createAuthenticationSASL() {
        ScramClient scramClient = ScramClient
            .channelBinding(NO)
            .stringPreparation(NO_PREPARATION)
            .selectMechanismBasedOnServerAdvertised("SCRAM-SHA-256")
            .setup();

        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new AuthenticationSASL(Collections.singletonList("SCRAM-SHA-256")))
                .expectRequest(new SASLInitialResponse(ByteBufferUtils.encode(scramClient.scramSession("test-username").clientFirstMessage()), "SCRAM-SHA-256")).thenRespond(AuthenticationOk.INSTANCE)
                .done()
            .build();
        // @formatter:on

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .database("test-database")
            .host("test-host")
            .username("test-username")
            .password("test-password")
            .build();
    }

    @Test
    void createError() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new ErrorResponse(Collections.emptyList()))
                .done()
            .build();
        // @formatter:on

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .database("test-database")
            .host("test-host")
            .username("test-username")
            .password("test-password")
            .build();

        new PostgresqlConnectionFactory(Mono.just(client), configuration).create()
            .as(StepVerifier::create)
            .verifyErrorMatches(PostgresqlServerErrorException.class::isInstance);
    }

    @Test
    void getMetadata() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new StartupMessage("test-application-name", "test-database", "test-username")).thenRespond(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))
                .expectRequest(new PasswordMessage("md55e9836cdb369d50e3bc7d127e88b4804")).thenRespond(AuthenticationOk.INSTANCE)
                .done()
            .build();
        // @formatter:on

        PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
            .applicationName("test-application-name")
            .database("test-database")
            .host("test-host")
            .username("test-username")
            .password("test-password")
            .build();

        assertThat(new PostgresqlConnectionFactory(Mono.just(client), configuration).getMetadata()).isNotNull();
    }

}
