package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.MultipleHostsConfiguration;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.r2dbc.postgresql.TargetServerType.ANY;
import static io.r2dbc.postgresql.TargetServerType.MASTER;
import static io.r2dbc.postgresql.TargetServerType.PREFER_SECONDARY;
import static io.r2dbc.postgresql.TargetServerType.SECONDARY;

class MultipleHostsClientFactory extends ClientFactoryBase {

    private final List<SocketAddress> addresses;

    private final MultipleHostsConfiguration configuration;

    private final Map<SocketAddress, HostSpecStatus> statusMap = new ConcurrentHashMap<>();

    public MultipleHostsClientFactory(PostgresqlConnectionConfiguration configuration, ClientSupplier clientSupplier) {
        super(configuration, clientSupplier);
        this.configuration = configuration.getMultipleHostsConfiguration();
        this.addresses = MultipleHostsClientFactory.createSocketAddress(this.configuration);
    }

    @Override
    public Mono<Client> create(@Nullable Map<String, String> options) {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        TargetServerType targetServerType = this.configuration.getTargetServerType();
        return this.tryConnect(targetServerType, options)
            .onErrorResume(e -> {
                if (!exceptionRef.compareAndSet(null, e)) {
                    exceptionRef.get().addSuppressed(e);
                }
                return Mono.empty();
            })
            .switchIfEmpty(Mono.defer(() -> targetServerType == PREFER_SECONDARY
                ? this.tryConnect(MASTER, options)
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

    public Mono<Client> tryConnect(TargetServerType targetServerType, @Nullable Map<String, String> options) {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        return this.getCandidates(targetServerType).concatMap(candidate -> this.tryConnectToCandidate(targetServerType, candidate, options)
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

    private static List<SocketAddress> createSocketAddress(MultipleHostsConfiguration configuration) {
        List<SocketAddress> addressList = new ArrayList<>(configuration.getHosts().size());
        for (MultipleHostsConfiguration.ServerHost host : configuration.getHosts()) {
            addressList.add(InetSocketAddress.createUnresolved(host.getHost(), host.getPort()));
        }
        return addressList;
    }

    private static HostSpecStatus evaluateStatus(SocketAddress candidate, @Nullable HostSpecStatus oldStatus) {
        return oldStatus == null || oldStatus.hostStatus == HostStatus.CONNECT_FAIL
            ? HostSpecStatus.ok(candidate)
            : oldStatus;
    }

    private static Mono<Boolean> isPrimaryServer(Client client) {
        return new SimpleQueryPostgresqlStatement(client, new DefaultCodecs(client.getByteBufAllocator()), "show transaction_read_only")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, String.class)))
            .map(s -> s.equalsIgnoreCase("off"))
            .next();
    }

    private Flux<SocketAddress> getCandidates(TargetServerType targetServerType) {
        return Flux.create(sink -> {
            if (this.addresses.size() == 1) {
                sink.next(this.addresses.get(0));
                sink.complete();
                return;
            }
            long now = System.currentTimeMillis();
            List<SocketAddress> addresses = new ArrayList<>(this.addresses);
            if (this.configuration.isLoadBalance()) {
                Collections.shuffle(addresses);
            }
            for (SocketAddress address : addresses) {
                HostSpecStatus currentStatus = this.statusMap.get(address);
                if (currentStatus == null || now > currentStatus.updated + this.configuration.getHostRecheckTime()) {
                    sink.next(address);
                } else if (targetServerType.allowStatus(currentStatus.hostStatus)) {
                    sink.next(address);
                }
            }
            sink.complete();
        });
    }

    private Mono<Client> tryConnectToCandidate(TargetServerType targetServerType, SocketAddress candidate, @Nullable Map<String, String> options) {
        return Mono.create(sink -> this.tryConnectToEndpoint(candidate, options).subscribe(client -> {
            this.statusMap.compute(candidate, (a, oldStatus) -> MultipleHostsClientFactory.evaluateStatus(candidate, oldStatus));
            if (targetServerType == ANY) {
                sink.success(client);
                return;
            }
            MultipleHostsClientFactory.isPrimaryServer(client).subscribe(
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
                        client.close().subscribe(v -> sink.success(), sink::error, sink::success, sink.currentContext());
                    }
                },
                sink::error,
                () -> {
                },
                sink.currentContext()
            );
        }, sink::error, () -> {
        }, sink.currentContext()));
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

        public final long updated = System.currentTimeMillis();

        private HostSpecStatus(SocketAddress address, HostStatus hostStatus) {
            this.address = address;
            this.hostStatus = hostStatus;
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
