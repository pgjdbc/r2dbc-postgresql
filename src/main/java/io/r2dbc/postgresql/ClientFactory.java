package io.r2dbc.postgresql;

import io.netty.channel.unix.DomainSocketAddress;
import io.r2dbc.postgresql.authentication.AuthenticationHandler;
import io.r2dbc.postgresql.authentication.PasswordAuthenticationHandler;
import io.r2dbc.postgresql.authentication.SASLAuthenticationHandler;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.SSLConfig;
import io.r2dbc.postgresql.client.SSLMode;
import io.r2dbc.postgresql.client.StartupMessageFlow;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.TargetServerType.ANY;
import static io.r2dbc.postgresql.TargetServerType.MASTER;
import static io.r2dbc.postgresql.TargetServerType.PREFER_SECONDARY;
import static io.r2dbc.postgresql.TargetServerType.SECONDARY;

class ClientFactory implements Function<Map<String, String>, Mono<? extends Client>> {

    private final List<SocketAddress> addresses;

    private final PostgresqlConnectionConfiguration configuration;

    private final Map<SocketAddress, HostSpecStatus> statusMap = new ConcurrentHashMap<>();

    private final ConnectionSupplier connectionSupplier;

    public ClientFactory(PostgresqlConnectionConfiguration configuration, ConnectionSupplier connectionSupplier) {
        this.configuration = configuration;
        this.addresses = ClientFactory.createSocketAddress(this.configuration);
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public Mono<Client> apply(Map<String, String> options) {
        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        TargetServerType targetServerType = this.configuration.getTargetServerType();
        return this.tryConnect(targetServerType, options)
            .onErrorResume(e -> this.addresses.size() > 1, e -> {
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
            .onErrorResume(e -> this.addresses.size() > 1, e -> {
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

    private static List<SocketAddress> createSocketAddress(PostgresqlConnectionConfiguration configuration) {
        if (!configuration.isUseSocket()) {
            if (configuration.getTmpHosts() != null) {
                String[] hosts = configuration.getTmpHosts();
                int[] ports = configuration.getTmpPorts();
                List<SocketAddress> addressList = new ArrayList<>(hosts.length);
                for (int i = 0; i < hosts.length; i++) {
                    String host = hosts[i];
                    int port = ports[i];
                    addressList.add(InetSocketAddress.createUnresolved(host, port));
                }
                return addressList;
            } else {
                return Collections.singletonList(InetSocketAddress.createUnresolved(configuration.getRequiredHost(), configuration.getPort()));
            }
        }

        if (configuration.isUseSocket()) {
            return Collections.singletonList(new DomainSocketAddress(configuration.getRequiredSocket()));
        }

        throw new IllegalArgumentException("Cannot create SocketAddress for " + configuration);
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

    private AuthenticationHandler getAuthenticationHandler(AuthenticationMessage message) {
        if (PasswordAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new PasswordAuthenticationHandler(password, this.configuration.getUsername());
        } else if (SASLAuthenticationHandler.supports(message)) {
            CharSequence password = Assert.requireNonNull(this.configuration.getPassword(), "Password must not be null");
            return new SASLAuthenticationHandler(password, this.configuration.getUsername());
        } else {
            throw new IllegalStateException(String.format("Unable to provide AuthenticationHandler capable of handling %s", message));
        }
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

    private Mono<Client> tryConnectWithConfig(SSLConfig sslConfig, SocketAddress endpoint, @Nullable Map<String, String> options) {
        return this.connectionSupplier.connect(endpoint, this.configuration.getConnectTimeout(), sslConfig)
            .delayUntil(client -> StartupMessageFlow
                .exchange(this.configuration.getApplicationName(), this::getAuthenticationHandler, client, this.configuration.getDatabase(), this.configuration.getUsername(), options)
                .handle(ExceptionFactory.INSTANCE::handleErrorResponse));
    }

    private Mono<Client> tryConnectToCandidate(TargetServerType targetServerType, SocketAddress candidate, @Nullable Map<String, String> options) {
        return Mono.create(sink -> this.tryConnectToEndpoint(candidate, options).subscribe(client -> {
            this.statusMap.compute(candidate, (a, oldStatus) -> ClientFactory.evaluateStatus(candidate, oldStatus));
            if (targetServerType == ANY) {
                sink.success(client);
                return;
            }
            ClientFactory.isPrimaryServer(client).subscribe(
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

    private Mono<Client> tryConnectToEndpoint(SocketAddress endpoint, @Nullable Map<String, String> options) {
        SSLConfig sslConfig = this.configuration.getSslConfig();
        Predicate<Throwable> isAuthSpecificationError = e -> e instanceof ExceptionFactory.PostgresqlAuthenticationFailure;
        return this.tryConnectWithConfig(sslConfig, endpoint, options)
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.ALLOW),
                e -> this.tryConnectWithConfig(sslConfig.mutateMode(SSLMode.REQUIRE), endpoint, options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            )
            .onErrorResume(
                isAuthSpecificationError.and(e -> sslConfig.getSslMode() == SSLMode.PREFER),
                e -> this.tryConnectWithConfig(sslConfig.mutateMode(SSLMode.DISABLE), endpoint, options)
                    .onErrorResume(sslAuthError -> {
                        e.addSuppressed(sslAuthError);
                        return Mono.error(e);
                    })
            );
    }

    public interface ConnectionSupplier {

        Mono<Client> connect(SocketAddress endpoint, @Nullable Duration connectTimeout, SSLConfig sslConfig);
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
