/*
 * Copyright 2022 the original author or authors.
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

import io.r2dbc.postgresql.MultiHostConnectionStrategy;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.util.PostgresqlHighAvailabilityClusterExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for multi-node Postgres server topologies.
 */
final class HighAvailabilityClusterIntegrationTests {

    @RegisterExtension
    static final PostgresqlHighAvailabilityClusterExtension SERVERS = new PostgresqlHighAvailabilityClusterExtension();

    @Test
    void testPrimaryAndStandbyStartup() {
        assertThat(SERVERS.getPrimaryJdbcTemplate().queryForObject("SHOW TRANSACTION_READ_ONLY", Boolean.class)).isFalse();
        assertThat(SERVERS.getStandbyJdbcTemplate().queryForObject("SHOW TRANSACTION_READ_ONLY", Boolean.class)).isTrue();
    }

    @Test
    void testMultipleCallsOnSameFactory() {
        PostgresqlConnectionFactory connectionFactory = this.multiHostConnectionFactory(MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby());

        Mono.usingWhen(connectionFactory.create(), this::isPrimary, Connection::close)
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        Mono.usingWhen(connectionFactory.create(), this::isPrimary, Connection::close)
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetAnyChooseFirst() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.ANY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.ANY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetAnyConnectedToPrimary() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.ANY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetAnyConnectedToStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.ANY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryChooseStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryConnectedToPrimary() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryConnectedToStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryChoosePrimary() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PRIMARY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PRIMARY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryConnectedOnPrimary() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PRIMARY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryFailedOnStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.PRIMARY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .verifyError(R2dbcNonTransientResourceException.class);
    }

    @Test
    void testTargetSecondaryChooseStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.SECONDARY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetSecondaryConnectedOnStandby() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.SECONDARY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetSecondaryFailedOnPrimary() {
        isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType.SECONDARY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .verifyError(R2dbcException.class);
    }

    private Mono<Boolean> isConnectedToPrimary(MultiHostConnectionStrategy.TargetServerType targetServerType, PostgreSQLContainer<?>... servers) {
        PostgresqlConnectionFactory connectionFactory = this.multiHostConnectionFactory(targetServerType, servers);

        return Mono.usingWhen(connectionFactory.create(), this::isPrimary, Connection::close);
    }

    private Mono<Boolean> isPrimary(PostgresqlConnection connection) {
        return connection.createStatement("SHOW TRANSACTION_READ_ONLY")
            .execute()
            .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
            .map(str -> str.equalsIgnoreCase("off"))
            .next();
    }

    private PostgresqlConnectionFactory multiHostConnectionFactory(MultiHostConnectionStrategy.TargetServerType targetServerType, PostgreSQLContainer<?>... servers) {
        PostgreSQLContainer<?> firstServer = servers[0];
        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder();
        for (PostgreSQLContainer<?> server : servers) {
            builder.addHost(server.getContainerIpAddress(), server.getMappedPort(5432));
        }
        PostgresqlConnectionConfiguration configuration = builder
            .targetServerType(targetServerType)
            .username(firstServer.getUsername())
            .password(firstServer.getPassword())
            .build();
        return new PostgresqlConnectionFactory(configuration);
    }

}
