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

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ConnectionSettings;
import io.r2dbc.postgresql.client.MultiHostConfiguration;
import io.r2dbc.postgresql.codec.DefaultCodecs;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.MultiHostConnectionStrategy.TargetServerType.ANY;
import static io.r2dbc.postgresql.MultiHostConnectionStrategy.TargetServerType.PREFER_SECONDARY;
import static io.r2dbc.postgresql.MultiHostConnectionStrategy.TargetServerType.PRIMARY;

/**
 * {@link ConnectionStrategy} using a collection of
 */
public final class MultiHostConnectionStrategy implements ConnectionStrategy {

    private final ConnectionFunction connectionFunction;

    private final Collection<SocketAddress> addresses;

    private final PostgresqlConnectionConfiguration configuration;

    private final MultiHostConfiguration multiHostConfiguration;

    private final ConnectionSettings settings;

    private final Map<SocketAddress, HostConnectOutcome> statusMap;

    MultiHostConnectionStrategy(ConnectionFunction connectionFunction, Collection<SocketAddress> addresses, PostgresqlConnectionConfiguration configuration, ConnectionSettings settings) {

        Assert.isTrue(!addresses.isEmpty(), "Collection of SocketAddress must not be empty");

        this.connectionFunction = connectionFunction;
        this.addresses = addresses;
        this.configuration = configuration;
        this.multiHostConfiguration = this.configuration.getMultiHostConfiguration();
        this.settings = settings;
        this.statusMap = new ConcurrentHashMap<>(addresses.size());
    }

    @Override
    public Mono<Client> connect() {
        return connect(this.multiHostConfiguration.getTargetServerType());
    }

    @Override
    public String toString() {
        return String.format("a %s%s", this.multiHostConfiguration.getTargetServerType() + " node using " + this.multiHostConfiguration.getHosts(), this.statusMap.isEmpty() ? "" :
            ". Known server states: " + this.statusMap);
    }

    public Mono<Client> connect(TargetServerType targetServerType) {
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        return attemptConnection(targetServerType, errors::add)
            .onErrorResume(e -> {
                errors.add(e);
                return Mono.empty();
            })
            .switchIfEmpty(Mono.defer(() -> targetServerType == PREFER_SECONDARY ? attemptConnection(PRIMARY, errors::add) : Mono.empty()))
            .switchIfEmpty(Mono.error(() -> {
                if (errors.isEmpty()) {
                    return new ExceptionAggregator(String.format("No server matches target type '%s'", targetServerType), null);
                } else {
                    RuntimeException exception = new ExceptionAggregator(null, errors.size() == 1 ?
                        errors.get(0) : null);

                    if (errors.size() > 1) {
                        errors.forEach(exception::addSuppressed);
                    }
                    return exception;
                }
            }));
    }

    private Mono<Client> attemptConnection(TargetServerType targetServerType, Consumer<Throwable> errorHandler) {
        return getCandidates(targetServerType).concatMap(candidate -> this.attemptConnection(targetServerType, candidate)
                .onErrorResume(e -> {
                    errorHandler.accept(e);
                    this.statusMap.put(candidate, HostConnectOutcome.fail(candidate));
                    return Mono.empty();
                }))
            .next();
    }

    private Mono<Client> attemptConnection(TargetServerType targetServerType, SocketAddress candidate) {

        return this.connectionFunction.connect(candidate, this.settings).flatMap(client -> {

            this.statusMap.compute(candidate, (a, oldStatus) -> evaluateStatus(candidate, oldStatus));

            if (targetServerType == ANY) {
                return Mono.just(client);
            }

            return isPrimaryServer(client, this.configuration).flatMap(
                isPrimary -> {

                    HostConnectOutcome outcome;
                    if (isPrimary) {
                        outcome = HostConnectOutcome.primary(candidate);
                    } else {
                        outcome = HostConnectOutcome.standby(candidate);
                    }

                    this.statusMap.put(candidate, outcome);

                    if (targetServerType.test(candidate, outcome.hostStatus)) {
                        return Mono.just(client);
                    }

                    return client.close().then(Mono.empty());
                });
        });

    }

    private static HostConnectOutcome evaluateStatus(SocketAddress candidate, @Nullable HostConnectOutcome oldStatus) {
        return oldStatus == null || oldStatus.hostStatus == HostStatus.CONNECT_FAIL
            ? HostConnectOutcome.ok(candidate) : oldStatus;
    }

    private static Mono<Boolean> isPrimaryServer(Client client, PostgresqlConnectionConfiguration configuration) {

        PostgresqlConnection connection = new PostgresqlConnection(client, new DefaultCodecs(client.getByteBufAllocator()), DefaultPortalNameSupplier.INSTANCE,
            DisabledStatementCache.INSTANCE, IsolationLevel.READ_UNCOMMITTED, configuration);

        return new io.r2dbc.postgresql.PostgresqlStatement(connection.getResources(), "SHOW TRANSACTION_READ_ONLY")
            .fetchSize(0)
            .execute()
            .flatMap(result -> result.map((row) -> row.get(0, String.class)))
            .map(s -> s.equalsIgnoreCase("off"))
            .last();
    }

    private Flux<SocketAddress> getCandidates(TargetServerType targetServerType) {

        return Flux.defer(() -> {

            Instant recheckIfBefore = HostConnectOutcome.DEFAULT_CLOCK.instant().plus(this.multiHostConfiguration.getHostRecheckTime());
            Predicate<Instant> needsRecheck = updated -> updated.isBefore(recheckIfBefore);

            List<SocketAddress> addresses = new ArrayList<>(this.addresses);
            List<SocketAddress> result = new ArrayList<>(this.addresses.size());

            if (this.multiHostConfiguration.isLoadBalanceHosts()) {
                Collections.shuffle(addresses);
            }

            for (SocketAddress address : addresses) {
                HostConnectOutcome currentStatus = this.statusMap.get(address);
                if (currentStatus == null || currentStatus.hostStatus == HostStatus.CONNECT_OK || needsRecheck.test(currentStatus.connectionAttemptedAt) || targetServerType.test(address,
                    currentStatus.hostStatus)) {

                    result.add(address);
                }
            }

            if (result.isEmpty()) {
                // if no candidate matches the requirement or all of them are in unavailable status, try all the hosts
                result = addresses;
            }

            return Flux.fromIterable(result);
        });

    }

    /**
     * Connection status for a host.
     */
    public enum HostStatus {

        CONNECT_FAIL,
        CONNECT_OK,
        PRIMARY,
        STANDBY
    }

    /**
     * Interface specifying a predicate whether to accept a given host based on its {@link SocketAddress} and {@link HostStatus}.
     */
    public interface HostSelector {

        /**
         * Perform a check and return {@code true} whether the given host qualifies as target server.
         *
         * @param address    must not be {@code null}
         * @param hostStatus must not be {@code null}
         * @return {@code true} if the given host qualifies as target server; {@code false} otherwise.
         */
        boolean test(SocketAddress address, HostStatus hostStatus);

    }

    static class ExceptionAggregator extends RuntimeException {

        public ExceptionAggregator(@Nullable String message, @Nullable Throwable cause) {
            super(message, cause);
        }
    }

    private static class HostConnectOutcome {

        static final Clock DEFAULT_CLOCK = Clock.systemDefaultZone();

        public final SocketAddress address;

        public final HostStatus hostStatus;

        public final Instant connectionAttemptedAt;

        private HostConnectOutcome(SocketAddress address, HostStatus hostStatus, Clock clock) {
            this.address = address;
            this.hostStatus = hostStatus;
            this.connectionAttemptedAt = clock.instant();
        }

        public static HostConnectOutcome fail(SocketAddress host) {
            return new HostConnectOutcome(host, HostStatus.CONNECT_FAIL, DEFAULT_CLOCK);
        }

        public static HostConnectOutcome ok(SocketAddress host) {
            return new HostConnectOutcome(host, HostStatus.CONNECT_OK, DEFAULT_CLOCK);
        }

        public static HostConnectOutcome primary(SocketAddress host) {
            return new HostConnectOutcome(host, HostStatus.PRIMARY, DEFAULT_CLOCK);
        }

        public static HostConnectOutcome standby(SocketAddress host) {
            return new HostConnectOutcome(host, HostStatus.STANDBY, DEFAULT_CLOCK);
        }

        @Override
        public String toString() {
            return this.hostStatus.name();
        }

    }

    /**
     * Pre-defined enumeration providing {@link HostSelector} implementations.
     *
     * @since 1.0
     */
    public enum TargetServerType implements HostSelector {

        /**
         * Any valid server that the driver was able to connect to.
         */
        ANY("any") {
            @Override
            public boolean test(SocketAddress address, HostStatus hostStatus) {
                return hostStatus != HostStatus.CONNECT_FAIL;
            }
        },

        /**
         * A master server whose initial {@code TRANSACTION_READ_ONLY} setting is {@code OFF}.
         */
        PRIMARY("primary") {
            @Override
            public boolean test(SocketAddress address, HostStatus hostStatus) {
                return hostStatus == HostStatus.PRIMARY;
            }
        },

        /**
         * A secondary server whose initial {@code TRANSACTION_READ_ONLY} setting is {@code ON}.
         */
        SECONDARY("secondary") {
            @Override
            public boolean test(SocketAddress address, HostStatus hostStatus) {
                return hostStatus == HostStatus.STANDBY;
            }
        },

        /**
         * A {@link #SECONDARY} server. If there is no {@link #SECONDARY} server available, fall back to {@link #PRIMARY}.
         */
        PREFER_SECONDARY("preferSecondary") {
            @Override
            public boolean test(SocketAddress address, HostStatus hostStatus) {
                return hostStatus == HostStatus.STANDBY;
            }
        };

        private final String value;

        TargetServerType(String value) {
            this.value = value;
        }

        public static TargetServerType fromValue(String value) {

            for (TargetServerType type : TargetServerType.values()) {
                if (type.value.equalsIgnoreCase(value) || type.name().equalsIgnoreCase(value)) {
                    return type;
                }
            }

            throw new IllegalArgumentException(String.format("Cannot resolve '%s' to a valid TargetServerType.", value));
        }

        public String getValue() {
            return this.value;
        }

    }

}
