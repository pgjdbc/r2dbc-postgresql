/*
 * Copyright 2017-2018 the original author or authors.
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

package io.r2dbc.postgresql.client;

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

import java.util.Collections;
import java.util.List;
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
    public static final Pattern PARAMETER_SYMBOL = Pattern.compile(".*\\$([\\d]+).*");

    private ExtendedQueryMessageFlow() {
    }

    /**
     * Execute the execute portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a> message flow.
     *
     * @param bindings           the {@link Binding}s to bind
     * @param client             the {@link Client} to exchange messages with
     * @param portalNameSupplier supplier unique portal names for each binding
     * @param statement          the name of the statement to execute
     * @return the messages received in response to the exchange
     * @throws IllegalArgumentException if {@code bindings}, {@code client}, {@code portalNameSupplier}, or {@code statement} is {@code null}
     */
    public static Flux<BackendMessage> execute(Publisher<Binding> bindings, Client client, PortalNameSupplier portalNameSupplier, String statement) {
        Assert.requireNonNull(bindings, "bindings must not be null");
        Assert.requireNonNull(client, "client must not be null");
        Assert.requireNonNull(portalNameSupplier, "portalNameSupplier must not be null");
        Assert.requireNonNull(statement, "statement must not be null");

        return client.exchange(Flux.from(bindings)
            .flatMap(binding -> toBindFlow(binding, portalNameSupplier, statement))
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

        return client.exchange(Flux.just(new Parse(name, types, query), new Describe(name, STATEMENT), Sync.INSTANCE))
            .takeUntil(or(RowDescription.class::isInstance, NoData.class::isInstance));
    }

    private static Flux<FrontendMessage> toBindFlow(Binding binding, PortalNameSupplier portalNameSupplier, String statement) {
        String portal = portalNameSupplier.get();

        // TODO: Specify Return Types
        Bind bind = new Bind(portal, binding.getParameterFormats(), binding.getParameterValues(), Collections.emptyList(), statement);

        return Flux.just(bind, new Describe(portal, PORTAL), new Execute(portal, NO_LIMIT), new Close(portal, PORTAL));
    }

}
