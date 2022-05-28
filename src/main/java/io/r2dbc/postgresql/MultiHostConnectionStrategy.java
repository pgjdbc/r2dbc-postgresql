package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.TargetServerType.ANY;
import static io.r2dbc.postgresql.TargetServerType.MASTER;
import static io.r2dbc.postgresql.TargetServerType.PREFER_SECONDARY;
import static io.r2dbc.postgresql.TargetServerType.SECONDARY;

public class MultiHostConnectionStrategy implements ConnectionStrategy {

    private final List<SocketAddress> addresses;

    private final PostgresqlConnectionConfiguration configuration;

    private final ComposableConnectionStrategy connectionStrategy;

    private final MultiHostConfiguration multiHostConfiguration;

    private final Map<SocketAddress, HostSpecStatus> statusMap;

    MultiHostConnectionStrategy(List<SocketAddress> addresses, PostgresqlConnectionConfiguration configuration, ComposableConnectionStrategy connectionStrategy) {
        this.addresses = addresses;
        this.configuration = configuration;
        this.connectionStrategy = connectionStrategy;
        this.multiHostConfiguration = this.configuration.getMultiHostConfiguration();
        this.statusMap = new ConcurrentHashMap<>();
    }

    @Override
    public Mono<Client> connect() {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        TargetServerType targetServerType = this.multiHostConfiguration.getTargetServerType();
        return this.tryConnect(targetServerType)
            .onErrorResume(e -> {
                if (!exceptionRef.compareAndSet(null, e)) {
                    exceptionRef.get().addSuppressed(e);
                }
                return Mono.empty();
            })
            .switchIfEmpty(Mono.defer(() -> targetServerType == PREFER_SECONDARY
                ? this.tryConnect(MASTER)
                : Mono.empty()))
            .switchIfEmpty(Mono.error(() -> {
                Throwable error = exceptionRef.get();
                if (error == null) {
                    return new PostgresqlConnectionFactory.PostgresConnectionException(String.format("No server matches target type %s", targetServerType.getValue()), null);
                } else {
                    return error;
                }
            }));
    }

    @Override
    public ConnectionStrategy withOptions(Map<String, String> options) {
        return new MultiHostConnectionStrategy(this.addresses, this.configuration, this.connectionStrategy.withOptions(options));
    }

    private Mono<Client> tryConnect(TargetServerType targetServerType) {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        return this.getCandidates(targetServerType).concatMap(candidate -> this.tryConnectToCandidate(targetServerType, candidate)
                .onErrorResume(e -> {
                    if (!exceptionRef.compareAndSet(null, e)) {
                        exceptionRef.get().addSuppressed(e);
                    }
                    this.statusMap.put(candidate, HostSpecStatus.fail(candidate));
                    return Mono.empty();
                }))
            .next()
            .switchIfEmpty(Mono.defer(() -> exceptionRef.get() != null
                ? Mono.error(exceptionRef.get())
                : Mono.empty()));
    }

    private static HostSpecStatus evaluateStatus(SocketAddress candidate, @Nullable HostSpecStatus oldStatus) {
        return oldStatus == null || oldStatus.hostStatus == HostStatus.CONNECT_FAIL
            ? HostSpecStatus.ok(candidate)
            : oldStatus;
    }

    private static Mono<Boolean> isPrimaryServer(Client client, PostgresqlConnectionConfiguration configuration) {
        PostgresqlConnection connection = new PostgresqlConnection(client, new DefaultCodecs(client.getByteBufAllocator()), DefaultPortalNameSupplier.INSTANCE,
            StatementCache.fromPreparedStatementCacheQueries(client, configuration.getPreparedStatementCacheQueries()), IsolationLevel.READ_UNCOMMITTED, configuration);
        return connection.createStatement("show transaction_read_only")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class)))
            .map(s -> s.equalsIgnoreCase("off"))
            .next();
    }

    private Flux<SocketAddress> getCandidates(TargetServerType targetServerType) {
        return Flux.create(sink -> {
            Predicate<Long> needsRecheck = updated -> System.currentTimeMillis() > updated + this.multiHostConfiguration.getHostRecheckTime().toMillis();
            List<SocketAddress> addresses = new ArrayList<>(this.addresses);
            if (this.multiHostConfiguration.isLoadBalanceHosts()) {
                Collections.shuffle(addresses);
            }
            boolean addressEmitted = false;
            for (SocketAddress address : addresses) {
                HostSpecStatus currentStatus = this.statusMap.get(address);
                if (currentStatus == null || needsRecheck.test(currentStatus.updated) || targetServerType.allowStatus(currentStatus.hostStatus)) {
                    sink.next(address);
                    addressEmitted = true;
                }
            }
            if (!addressEmitted) {
                // if no candidate matches the requirement or all of them are in unavailable status, try all the hosts
                for (SocketAddress address : addresses) {
                    sink.next(address);
                }
            }
            sink.complete();
        });
    }

    private Mono<Client> tryConnectToCandidate(TargetServerType targetServerType, SocketAddress candidate) {
        return Mono.create(sink -> this.connectionStrategy.withAddress(candidate).connect().subscribe(client -> {
            this.statusMap.compute(candidate, (a, oldStatus) -> evaluateStatus(candidate, oldStatus));
            if (targetServerType == ANY) {
                sink.success(client);
                return;
            }
            isPrimaryServer(client, this.configuration).subscribe(
                isPrimary -> {
                    if (isPrimary) {
                        this.statusMap.put(candidate, HostSpecStatus.primary(candidate));
                    } else {
                        this.statusMap.put(candidate, HostSpecStatus.standby(candidate));
                    }
                    if (isPrimary && targetServerType == MASTER) {
                        sink.success(client);
                    } else if (!isPrimary && (targetServerType == SECONDARY || targetServerType == PREFER_SECONDARY)) {
                        sink.success(client);
                    } else {
                        client.close().subscribe(v -> sink.success(), sink::error, sink::success, Context.of(sink.contextView()));
                    }
                },
                sink::error, () -> {}, Context.of(sink.contextView()));
        }, sink::error, () -> {}, Context.of(sink.contextView())));
    }

    enum HostStatus {
        CONNECT_FAIL,
        CONNECT_OK,
        PRIMARY,
        STANDBY
    }

    private static class HostSpecStatus {

        public final SocketAddress address;

        public final HostStatus hostStatus;

        public final long updated;

        private HostSpecStatus(SocketAddress address, HostStatus hostStatus) {
            this.address = address;
            this.hostStatus = hostStatus;
            this.updated = System.currentTimeMillis();
        }

        public static HostSpecStatus fail(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.CONNECT_FAIL);
        }

        public static HostSpecStatus ok(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.CONNECT_OK);
        }

        public static HostSpecStatus primary(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.PRIMARY);
        }

        public static HostSpecStatus standby(SocketAddress host) {
            return new HostSpecStatus(host, HostStatus.STANDBY);
        }
    }
}
