/*
 * Copyright 2020 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ExtendedQueryMessageFlow;
import io.r2dbc.postgresql.client.QueryLogger;
import io.r2dbc.postgresql.client.TransactionStatus;
import io.r2dbc.postgresql.message.backend.BackendMessage;
import io.r2dbc.postgresql.message.backend.BindComplete;
import io.r2dbc.postgresql.message.backend.CloseComplete;
import io.r2dbc.postgresql.message.backend.CommandComplete;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.NoData;
import io.r2dbc.postgresql.message.backend.ParseComplete;
import io.r2dbc.postgresql.message.backend.PortalSuspended;
import io.r2dbc.postgresql.message.backend.ReadyForQuery;
import io.r2dbc.postgresql.message.frontend.Bind;
import io.r2dbc.postgresql.message.frontend.Close;
import io.r2dbc.postgresql.message.frontend.CompositeFrontendMessage;
import io.r2dbc.postgresql.message.frontend.Describe;
import io.r2dbc.postgresql.message.frontend.Execute;
import io.r2dbc.postgresql.message.frontend.Flush;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.Parse;
import io.r2dbc.postgresql.message.frontend.Sync;
import io.r2dbc.postgresql.util.Operators;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.message.frontend.Execute.NO_LIMIT;
import static io.r2dbc.postgresql.message.frontend.ExecutionType.PORTAL;
import static io.r2dbc.postgresql.util.PredicateUtils.not;
import static io.r2dbc.postgresql.util.PredicateUtils.or;

/**
 * Utility to execute the {@code Parse/Bind/Describe/Execute/Sync} portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a>
 * message flow.
 */
class ExtendedFlowDelegate {

    static final Predicate<BackendMessage> RESULT_FRAME_FILTER = not(or(BindComplete.class::isInstance, NoData.class::isInstance));

    /**
     * Execute the {@code Parse/Bind/Describe/Execute/Sync} portion of the <a href="https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY">Extended query</a>
     * message flow.
     *
     * @param resources the {@link ConnectionResources} providing access to the {@link Client}
     * @param factory   the {@link ExceptionFactory}
     * @param query     the query to execute
     * @param binding   the {@link Binding} to bind
     * @param values    the binding values
     * @param fetchSize the fetch size to apply. Use a single {@link Execute} with fetch all if {@code fetchSize} is zero. Otherwise, perform multiple roundtrips with smaller
     *                  {@link Execute} sizes.
     * @return the messages received in response to the exchange
     * @throws IllegalArgumentException if {@code bindings}, {@code client}, {@code portalNameSupplier}, or {@code statementName} is {@code null}
     */
    public static Flux<BackendMessage> runQuery(ConnectionResources resources, ExceptionFactory factory, String query, Binding binding, List<ByteBuf> values, int fetchSize) {

        StatementCache cache = resources.getStatementCache();
        Client client = resources.getClient();

        String name = cache.getName(binding, query);
        String portal = resources.getPortalNameSupplier().get();
        boolean prepareRequired = cache.requiresPrepare(binding, query);

        List<FrontendMessage.DirectEncoder> messagesToSend = new ArrayList<>(6);

        if (prepareRequired) {
            messagesToSend.add(new Parse(name, binding.getParameterTypes(), query));
        }

        Bind bind = new Bind(portal, binding.getParameterFormats(), values, ExtendedQueryMessageFlow.resultFormat(resources.getConfiguration().isForceBinary()), name);

        messagesToSend.add(bind);
        messagesToSend.add(new Describe(portal, PORTAL));

        Flux<BackendMessage> exchange;
        boolean compatibilityMode = resources.getConfiguration().isCompatibilityMode();
        boolean implicitTransactions = resources.getClient().getTransactionStatus() == TransactionStatus.IDLE;

        if (compatibilityMode) {

            if (fetchSize == NO_LIMIT || implicitTransactions) {
                exchange = fetchAll(messagesToSend, client, portal);
            } else {
                exchange = fetchCursoredWithSync(messagesToSend, client, portal, fetchSize);
            }
        } else {

            if (fetchSize == NO_LIMIT) {
                exchange = fetchAll(messagesToSend, client, portal);
            } else {
                exchange = fetchCursoredWithFlush(messagesToSend, client, portal, fetchSize);
            }
        }

        if (prepareRequired) {

            exchange = exchange.doOnNext(message -> {

                if (message == ParseComplete.INSTANCE) {
                    cache.put(binding, query, name);
                }
            });
        }

        return exchange.doOnSubscribe(it -> QueryLogger.logQuery(client.getContext(), query)).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release).filter(RESULT_FRAME_FILTER).handle(factory::handleErrorResponse);
    }

    /**
     * Execute the query and indicate to fetch all rows with the {@link Execute} message.
     *
     * @param messagesToSend the initial bind flow
     * @param client         client to use
     * @param portal         the portal
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchAll(List<FrontendMessage.DirectEncoder> messagesToSend, Client client, String portal) {

        messagesToSend.add(new Execute(portal, NO_LIMIT));
        messagesToSend.add(new Close(portal, PORTAL));
        messagesToSend.add(Sync.INSTANCE);

        return client.exchange(Mono.just(new CompositeFrontendMessage(messagesToSend)))
            .as(Operators::discardOnCancel);
    }

    /**
     * Execute a chunked query and indicate to fetch rows in chunks with the {@link Execute} message.
     *
     * @param messagesToSend the messages to send
     * @param client         client to use
     * @param portal         the portal
     * @param fetchSize      fetch size per roundtrip
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchCursoredWithSync(List<FrontendMessage.DirectEncoder> messagesToSend, Client client, String portal, int fetchSize) {

        DirectProcessor<FrontendMessage> requestsProcessor = DirectProcessor.create();
        FluxSink<FrontendMessage> requestsSink = requestsProcessor.sink();
        AtomicBoolean isCanceled = new AtomicBoolean(false);
        AtomicBoolean done = new AtomicBoolean(false);

        messagesToSend.add(new Execute(portal, fetchSize));
        messagesToSend.add(Sync.INSTANCE);

        return client.exchange(it -> done.get() && it instanceof ReadyForQuery, Flux.<FrontendMessage>just(new CompositeFrontendMessage(messagesToSend)).concatWith(requestsProcessor))
            .handle((BackendMessage message, SynchronousSink<BackendMessage> sink) -> {

                if (message instanceof CommandComplete) {
                    requestsSink.next(new Close(portal, PORTAL));
                    requestsSink.next(Sync.INSTANCE);
                    requestsSink.complete();
                    sink.next(message);
                } else if (message instanceof CloseComplete) {
                    requestsSink.complete();
                    done.set(true);
                    sink.next(message);
                } else if (message instanceof ErrorResponse) {
                    done.set(true);
                    requestsSink.next(Sync.INSTANCE);
                    requestsSink.complete();
                    sink.next(message);
                } else if (message instanceof PortalSuspended) {

                    if (isCanceled.get()) {
                        requestsSink.next(new Close(portal, PORTAL));
                        requestsSink.next(Sync.INSTANCE);
                        requestsSink.complete();
                    } else {
                        requestsSink.next(new Execute(portal, fetchSize));
                        requestsSink.next(Sync.INSTANCE);
                    }
                } else if (message instanceof NoData) {

                    if (isCanceled.get()) {
                        requestsSink.next(new Close(portal, PORTAL));
                        requestsSink.next(Sync.INSTANCE);
                        requestsSink.complete();
                    } else {
                        done.set(true);
                    }
                } else {
                    sink.next(message);
                }
            }).doFinally(ignore -> requestsSink.complete())
            .as(flux -> Operators.discardOnCancel(flux, () -> isCanceled.set(true)));
    }

    /**
     * Execute a contiguous query and indicate to fetch rows in chunks with the {@link Execute} message. Uses {@link Flush}-based synchronization that creates a cursor. Note that flushing keeps the
     * cursor open even with implicit transactions and this method may not work with newer pgpool implementations.
     *
     * @param messagesToSend the messages to send
     * @param client         client to use
     * @param portal         the portal
     * @param fetchSize      fetch size per roundtrip
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchCursoredWithFlush(List<FrontendMessage.DirectEncoder> messagesToSend, Client client, String portal, int fetchSize) {

        DirectProcessor<FrontendMessage> requestsProcessor = DirectProcessor.create();
        FluxSink<FrontendMessage> requestsSink = requestsProcessor.sink();
        AtomicBoolean isCanceled = new AtomicBoolean(false);

        messagesToSend.add(new Execute(portal, fetchSize));
        messagesToSend.add(Flush.INSTANCE);

        return client.exchange(Flux.<FrontendMessage>just(new CompositeFrontendMessage(messagesToSend)).concatWith(requestsProcessor))
            .handle((BackendMessage message, SynchronousSink<BackendMessage> sink) -> {

                if (message instanceof CommandComplete) {
                    requestsSink.next(new Close(portal, PORTAL));
                    requestsSink.next(Sync.INSTANCE);
                    requestsSink.complete();
                    sink.next(message);
                } else if (message instanceof ErrorResponse) {
                    requestsSink.next(Sync.INSTANCE);
                    requestsSink.complete();
                    sink.next(message);
                } else if (message instanceof PortalSuspended) {
                    if (isCanceled.get()) {
                        requestsSink.next(new Close(portal, PORTAL));
                        requestsSink.next(Sync.INSTANCE);
                        requestsSink.complete();
                    } else {
                        requestsSink.next(new Execute(portal, fetchSize));
                        requestsSink.next(Flush.INSTANCE);
                    }
                } else {
                    sink.next(message);
                }
            }).doFinally(ignore -> requestsSink.complete())
            .as(flux -> Operators.discardOnCancel(flux, () -> isCanceled.set(true)));
    }

}
