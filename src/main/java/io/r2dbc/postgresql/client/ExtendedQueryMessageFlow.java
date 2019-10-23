/*
 * Copyright 2017-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.NoData;
import io.r2dbc.postgresql.message.backend.RowDescription;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.PORTAL;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.STATEMENT;
import static io.r2dbc.postgresql.util.PredicateUtils.or;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
 */
public final class ExtendedQueryMessageFlow {

    /**
     * The pattern that identifies a parameter symbol.
     */
    public static final Pattern PARAMETER_SYMBOL = Pattern.compile("\\$([\\d]+)", Pattern.DOTALL);

    private static final Predicate<BackendMessage> TAKE_UNTIL = or(RowDescription.class::isInstance, NoData.class::isInstance);

    private ExtendedQueryMessageFlow() {
    }

    /**
     * Execute the execute portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
     *
     * @param bindings           the {@link Binding}s to bind
     * @param client             the {@link Client} to exchange messages with
     * @param portalNameSupplier supplier unique portal names for each binding
     * @param statementName      the name of the statementName to execute
     * @param query              the query to execute
     * @param forceBinary        force backend to return column data values in binary format for all columns
     * @return the messages received in response to the exchange
     * @throws IllegalArgumentException if {@code bindings}, {@code client}, {@code portalNameSupplier}, or {@code statementName} is {@code null}
     */
    public static Flux<BackendMessage> execute(Collection<Binding> bindings, Client client, PortalNameSupplier portalNameSupplier, String statementName, String query, boolean forceBinary) {
        Assert.requireNonNull(bindings, "bindings must not be null");
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        Assert.requireNonNull(statementName, "statementName must not be null");

        if (bindings.size() == 1) {

            Binding binding = bindings.iterator().next();
            return client.exchange(toBindFlow(binding, portalNameSupplier, statementName, query, forceBinary, true));
        } else {

            return client.exchange(Flux.fromIterable(bindings)
                .flatMap(binding -> toBindFlow(binding, portalNameSupplier, statementName, query, forceBinary, false))
                .concatWith(Mono.just(Sync.INSTANCE)));
        }
    }

    /**
     * Execute the parse portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
     *
     * @param client the {@link Client} to exchange messages with
     * @param name   the name of the statement to prepare
     * @param query  the query to execute
     * @param types  the parameter types for the query
     * @return the messages received in response to this exchange
     * @throws IllegalArgumentException if {@code client}, {@code name}, {@code query}, or {@code types} is {@code null}
     */
    public static Flux<BackendMessage> parse(Client client, String name, String query, int[] types) {
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(name, "name must not be null");
        Assert.requireNonNull(query, "query must not be null");
        Assert.requireNonNull(types, "types must not be null");

        return client.exchange(Flux.just(new Parse(name, types, query), new Describe(name, STATEMENT), Sync.INSTANCE))
            .takeUntil(TAKE_UNTIL);
    }

    private static Collection<Format> resultFormat(boolean forceBinary) {
        if (forceBinary) {
            return Format.binary();
        } else {
            return Collections.emptyList();
        }
    }

    private static Publisher<FrontendMessage> toBindFlow(Binding binding, PortalNameSupplier portalNameSupplier, String statementName, String query, boolean forceBinary, boolean inlineSync) {
        String portal = portalNameSupplier.get();
        return binding.parameterValues()
            .flatMap(f -> {
                if (f == Parameter.NULL_VALUE) {
                    return Flux.just(Bind.NULL_VALUE);
                } else if (f instanceof Mono) {
                    return f;
                } else {
                    return Flux.from(f)
                        .reduce(Unpooled.compositeBuffer(), (c, b) -> c.addComponent(true, b));
                }
            })
            .collectList()
            .map(values -> {
                Bind bind = new Bind(portal, binding.getParameterFormats(), values, resultFormat(forceBinary), statementName);

                if (inlineSync) {
                    return new BindDescribeExecuteSyncClose(bind, portal);
                }

                return new BindDescribeExecuteClose(bind, portal);
            }).doOnSubscribe(ignore -> QueryLogger.logQuery(query));
    }

    public static class BindDescribeExecuteSyncClose implements FrontendMessage {

        private final Bind bind;

        private final String portal;

        public BindDescribeExecuteSyncClose(Bind bind, String portal) {

            this.bind = bind;
            this.portal = portal;
        }

        @Override
        public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {

            return Mono.fromSupplier(() -> {

                ByteBuf buffer = byteBufAllocator.buffer(4 * 32);

                this.bind.encode(buffer);
                Describe.encode(buffer, this.portal, PORTAL);
                Execute.encode(buffer, this.portal, NO_LIMIT);
                Sync.INSTANCE.encode(buffer);
                Close.encode(buffer, this.portal, PORTAL);
                return buffer;
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BindDescribeExecuteSyncClose)) {
                return false;
            }
            BindDescribeExecuteSyncClose that = (BindDescribeExecuteSyncClose) o;
            return Objects.equals(this.bind, that.bind) &&
                Objects.equals(this.portal, that.portal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.bind, this.portal);
        }

        @Override
        public String toString() {
            return "BindDescExecSyncClose{" +
                "bind=" + this.bind +
                ", portal='" + this.portal + '\'' +
                '}';
        }
    }

    public static class BindDescribeExecuteClose implements FrontendMessage {

        private final Bind bind;

        private final String portal;

        public BindDescribeExecuteClose(Bind bind, String portal) {

            this.bind = bind;
            this.portal = portal;
        }

        @Override
        public Publisher<ByteBuf> encode(ByteBufAllocator byteBufAllocator) {

            return Mono.fromSupplier(() -> {

                ByteBuf buffer = byteBufAllocator.buffer(4 * 32);

                this.bind.encode(buffer);
                Describe.encode(buffer, this.portal, PORTAL);
                Execute.encode(buffer, this.portal, NO_LIMIT);
                Close.encode(buffer, this.portal, PORTAL);
                return buffer;
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BindDescribeExecuteClose)) {
                return false;
            }
            BindDescribeExecuteClose that = (BindDescribeExecuteClose) o;
            return Objects.equals(this.bind, that.bind) &&
                Objects.equals(this.portal, that.portal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.bind, this.portal);
        }

        @Override
        public String toString() {
            return "BindDescExecClose{" +
                "bind=" + this.bind +
                ", portal='" + this.portal + '\'' +
                '}';
        }
    }

}
