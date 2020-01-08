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

import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.ExecutionType;
import io.r2dbc.postgresql.message.frontend.Flush;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import io.r2dbc.postgresql.util.Assert;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.PORTAL;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
 */
public final class ExtendedQueryMessageFlow {

    /**
     * The pattern that identifies a parameter symbol.
     */
    public static final Pattern PARAMETER_SYMBOL = Pattern.compile("\\$([\\d]+)", Pattern.DOTALL);

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
    public static Flux<BackendMessage> execute(Publisher<Binding> bindings, Client client, PortalNameSupplier portalNameSupplier, String statementName, String query, boolean forceBinary) {
        Assert.requireNonNull(bindings, "bindings must not be null");
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        Assert.requireNonNull(statementName, "statementName must not be null");

        return client.exchange(Flux.from(bindings)
            .flatMap(binding -> toBindFlow(binding, portalNameSupplier, statementName, query, forceBinary))
            .concatWith(Mono.just(Sync.INSTANCE)));
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
    public static Flux<BackendMessage> parse(Client client, String name, String query, List<Integer> types) {
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(name, "name must not be null");
        Assert.requireNonNull(query, "query must not be null");
        Assert.requireNonNull(types, "types must not be null");

        /*
         ParseComplete will be received if parse was successful
         ReadyForQuery will be received as a response to Sync, which was send in case of error in parsing
         */
        return client.exchange(message -> message instanceof ParseComplete || message instanceof ReadyForQuery, Flux.just(new Parse(name, types, query), Flush.INSTANCE))
            .doOnNext(message -> {
                if (message instanceof ErrorResponse) {
                    /*
                    When an error is detected while processing any extended-query message, the backend issues ErrorResponse, then reads and discards messages until a Sync is reached.
                    So we have to provide Sync message to continue.
                    */
                    client.send(Sync.INSTANCE);
                }
            });
    }

    /**
     * Execute the close portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
     *
     * @param client the {@link Client} to exchange messages with
     * @param name   the name of the statement to close
     * @return the messages received in response to this exchange
     * @throws IllegalArgumentException if {@code client}, {@code name}, {@code query}, or {@code types} is {@code null}
     */
    public static Flux<BackendMessage> closeStatement(Client client, String name) {
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(name, "name must not be null");

        return client.exchange(Flux.just(new Close(name, ExecutionType.STATEMENT), Sync.INSTANCE))
            .takeUntil(CloseComplete.class::isInstance);
    }

    private static Collection<Format> resultFormat(boolean forceBinary) {
        if (forceBinary) {
            return Format.binary();
        } else {
            return Collections.emptyList();
        }
    }

    private static Flux<FrontendMessage> toBindFlow(Binding binding, PortalNameSupplier portalNameSupplier, String statementName, String query, boolean forceBinary) {
        String portal = portalNameSupplier.get();

        return Flux.fromIterable(binding.getParameterValues())
            .flatMap(f -> {
                if (f == Parameter.NULL_VALUE) {
                    return Flux.just(Bind.NULL_VALUE);
                } else {
                    return Flux.from(f)
                        .reduce(Unpooled.compositeBuffer(), (c, b) -> c.addComponent(true, b));
                }
            })
            .collectList()
            .flatMapMany(values -> {
                Bind bind = new Bind(portal, binding.getParameterFormats(), values, resultFormat(forceBinary), statementName);

                return Flux.just(bind, new Describe(portal, PORTAL), new Execute(portal, NO_LIMIT), new Close(portal, PORTAL));
            }).doOnSubscribe(ignore -> QueryLogger.logQuery(query));
    }

}
