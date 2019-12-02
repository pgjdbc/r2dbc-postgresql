package io.r2dbc.postgresql.client;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.TargetServerType;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.util.PostgresqlHighAvailabilityClusterExtension;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class HighAvailabilityClusterTest {

    @RegisterExtension
    static final PostgresqlHighAvailabilityClusterExtension SERVERS = new PostgresqlHighAvailabilityClusterExtension();

    @Test
    void testPrimaryAndStandbyStartup() {
        Assertions.assertFalse(SERVERS.getPrimaryJdbc().queryForObject("show transaction_read_only", Boolean.class));
        Assertions.assertTrue(SERVERS.getStandbyJdbc().queryForObject("show transaction_read_only", Boolean.class));
    }

    @Test
    void testMultipleCallsOnSameFactory() {
        PostgresqlConnectionFactory connectionFactory = this.multipleHostsConnectionFactory(TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby());

        connectionFactory
            .create()
            .flatMapMany(connection -> this.isPrimary(connection)
                .concatWith(connection.close().then(Mono.empty())))
            .next()
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        connectionFactory
            .create()
            .flatMapMany(connection -> this.isPrimary(connection)
                .concatWith(connection.close().then(Mono.empty())))
            .next()
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetAnyChooseFirst() {
        isConnectedToPrimary(TargetServerType.ANY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        isConnectedToPrimary(TargetServerType.ANY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetAnyConnectedToPrimary() {
        isConnectedToPrimary(TargetServerType.ANY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetAnyConnectedToStandby() {
        isConnectedToPrimary(TargetServerType.ANY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryChooseStandby() {
        isConnectedToPrimary(TargetServerType.PREFER_SECONDARY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        isConnectedToPrimary(TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryConnectedToPrimary() {
        isConnectedToPrimary(TargetServerType.PREFER_SECONDARY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPreferSecondaryConnectedToStandby() {
        isConnectedToPrimary(TargetServerType.PREFER_SECONDARY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryChoosePrimary() {
        isConnectedToPrimary(TargetServerType.MASTER, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();

        isConnectedToPrimary(TargetServerType.MASTER, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryConnectedOnPrimary() {
        isConnectedToPrimary(TargetServerType.MASTER, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void testTargetPrimaryFailedOnStandby() {
        isConnectedToPrimary(TargetServerType.MASTER, SERVERS.getStandby())
            .as(StepVerifier::create)
            .verifyError(R2dbcException.class);
    }

    @Test
    void testTargetSecondaryChooseStandby() {
        isConnectedToPrimary(TargetServerType.SECONDARY, SERVERS.getStandby(), SERVERS.getPrimary())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();

        isConnectedToPrimary(TargetServerType.SECONDARY, SERVERS.getPrimary(), SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetSecondaryConnectedOnStandby() {
        isConnectedToPrimary(TargetServerType.SECONDARY, SERVERS.getStandby())
            .as(StepVerifier::create)
            .expectNext(false)
            .verifyComplete();
    }

    @Test
    void testTargetSecondaryFailedOnPrimary() {
        isConnectedToPrimary(TargetServerType.SECONDARY, SERVERS.getPrimary())
            .as(StepVerifier::create)
            .verifyError(R2dbcException.class);
    }

    private Mono<Boolean> isConnectedToPrimary(TargetServerType targetServerType, PostgreSQLContainer<?>... servers) {
        PostgresqlConnectionFactory connectionFactory = this.multipleHostsConnectionFactory(targetServerType, servers);

        return connectionFactory
            .create()
            .flatMapMany(connection -> this.isPrimary(connection)
                .concatWith(connection.close().then(Mono.empty())))
            .next();
    }

    private Mono<Boolean> isPrimary(PostgresqlConnection connection) {
        return connection.createStatement("show transaction_read_only")
            .execute()
            .flatMap(result -> result.map((row, meta) -> row.get(0, String.class)))
            .map(str -> str.equalsIgnoreCase("off"))
            .next();
    }

    private PostgresqlConnectionFactory multipleHostsConnectionFactory(TargetServerType targetServerType, PostgreSQLContainer<?>... servers) {
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
