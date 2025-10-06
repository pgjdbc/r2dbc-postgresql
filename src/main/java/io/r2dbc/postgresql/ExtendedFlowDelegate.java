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
import io.r2dbc.postgresql.api.ErrorDetails;
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
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.util.concurrent.Queues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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
     * @param resources  the {@link ConnectionResources} providing access to the {@link Client}
     * @param factory    the {@link ExceptionFactory}
     * @param query      the query to execute
     * @param binding    the {@link Binding} to bind
     * @param values     the binding values
     * @param fetchSize  the fetch size to apply. Use a single {@link Execute} with fetch all if {@code fetchSize} is zero. Otherwise, perform multiple roundtrips with smaller
     *                   {@link Execute} sizes.
     * @param isCanceled whether the conversation is canceled
     * @return the messages received in response to the exchange
     * @throws IllegalArgumentException if {@code bindings}, {@code client}, {@code portalNameSupplier}, or {@code statementName} is {@code null}
     */
    public static Flux<BackendMessage> runQuery(ConnectionResources resources, ExceptionFactory factory, String query, Binding binding, List<ByteBuf> values, int fetchSize, AtomicBoolean isCanceled) {

        StatementCache cache = resources.getStatementCache();
        Client client = resources.getClient();
        String portal = resources.getPortalNameSupplier().get();

        Flux<BackendMessage> exchange;
        boolean compatibilityMode = resources.getConfiguration().isCompatibilityMode();
        boolean implicitTransactions = resources.getClient().getTransactionStatus() == TransactionStatus.IDLE;

        ExtendedFlowOperator operator = new ExtendedFlowOperator(query, binding, cache, values, portal, resources.getConfiguration().isForceBinary());

        if (compatibilityMode) {

            if (fetchSize == NO_LIMIT || implicitTransactions) {
                exchange = fetchAll(operator, client, portal);
            } else {
                exchange = fetchCursoredWithSync(operator, client, portal, fetchSize, isCanceled);
            }
        } else {

            if (fetchSize == NO_LIMIT) {
                exchange = fetchAll(operator, client, portal);
            } else {
                exchange = fetchCursoredWithFlush(operator, client, portal, fetchSize, isCanceled);
            }
        }

        exchange = exchange.doOnNext(message -> {

            if (message == ParseComplete.INSTANCE) {
                operator.hydrateStatementCache();
            }
        });

        return exchange.doOnSubscribe(it -> QueryLogger.logQuery(client.getContext(), query)).doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release).filter(RESULT_FRAME_FILTER).handle(factory::handleErrorResponse);
    }

    /**
     * Execute the query and indicate to fetch all rows with the {@link Execute} message.
     *
     * @param operator the flow operator
     * @param client   client to use
     * @param portal   the portal
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchAll(ExtendedFlowOperator operator, Client client, String portal) {

        Sinks.Many<FrontendMessage> requests = Sinks.many().unicast().onBackpressureBuffer(Queues.<FrontendMessage>small().get());
        MessageFactory factory = () -> operator.getMessages(Arrays.asList(new Execute(portal, NO_LIMIT), new Close(portal, PORTAL), Sync.INSTANCE));

        return client.exchange(operator.takeUntil(), Flux.<FrontendMessage>just(new CompositeFrontendMessage(factory.createMessages())).concatWith(requests.asFlux()))
            .handle(handleReprepare(requests, operator, factory))
            .doFinally(ignore -> operator.close(requests))
            .as(Operators::discardOnCancel);
    }

    /**
     * Execute a chunked query and indicate to fetch rows in chunks with the {@link Execute} message.
     *
     * @param operator   the flow operator
     * @param client     client to use
     * @param portal     the portal
     * @param fetchSize  fetch size per roundtrip
     * @param isCanceled whether the conversation is canceled
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchCursoredWithSync(ExtendedFlowOperator operator, Client client, String portal, int fetchSize, AtomicBoolean isCanceled) {

        Sinks.Many<FrontendMessage> requests = Sinks.many().unicast().onBackpressureBuffer(Queues.<FrontendMessage>small().get());
        AtomicBoolean done = new AtomicBoolean(false);

        MessageFactory factory = () -> operator.getMessages(Arrays.asList(new Execute(portal, fetchSize), Sync.INSTANCE));
        Predicate<BackendMessage> takeUntil = operator.takeUntil();

        return client.exchange(it -> done.get() && takeUntil.test(it), Flux.<FrontendMessage>just(new CompositeFrontendMessage(factory.createMessages())).concatWith(requests.asFlux()))
            .handle(handleReprepare(requests, operator, factory))
            .handle((BackendMessage message, SynchronousSink<BackendMessage> sink) -> {

                if (message instanceof CommandComplete) {
                    requests.emitNext(new Close(portal, PORTAL), Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.next(message);
                } else if (message instanceof CloseComplete) {
                    requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    done.set(true);
                    sink.next(message);
                } else if (message instanceof ErrorResponse) {
                    done.set(true);
                    requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.next(message);
                } else if (message instanceof PortalSuspended) {

                    if (isCanceled.get()) {
                        requests.emitNext(new Close(portal, PORTAL), Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    } else {
                        requests.emitNext(new Execute(portal, fetchSize), Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    }
                } else if (message instanceof NoData) {

                    if (isCanceled.get()) {
                        requests.emitNext(new Close(portal, PORTAL), Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    } else {
                        done.set(true);
                    }
                } else {
                    sink.next(message);
                }
            }).doFinally(ignore -> operator.close(requests))
            .as(flux -> Operators.discardOnCancel(flux, () -> isCanceled.set(true)));
    }

    /**
     * Execute a contiguous query and indicate to fetch rows in chunks with the {@link Execute} message. Uses {@link Flush}-based synchronization that creates a cursor. Note that flushing keeps the
     * cursor open even with implicit transactions and this method may not work with newer pgpool implementations.
     *
     * @param operator   the flow operator
     * @param client     client to use
     * @param portal     the portal
     * @param fetchSize  fetch size per roundtrip
     * @param isCanceled whether the conversation is canceled
     * @return the resulting message stream
     */
    private static Flux<BackendMessage> fetchCursoredWithFlush(ExtendedFlowOperator operator, Client client, String portal, int fetchSize, AtomicBoolean isCanceled) {

        Sinks.Many<FrontendMessage> requests = Sinks.many().unicast().onBackpressureBuffer(Queues.<FrontendMessage>small().get());

        MessageFactory factory = () -> operator.getMessages(Arrays.asList(new Execute(portal, fetchSize), Flush.INSTANCE));

        return client.exchange(operator.takeUntil(), Flux.<FrontendMessage>just(new CompositeFrontendMessage(factory.createMessages())).concatWith(requests.asFlux()))
            .handle(handleReprepare(requests, operator, factory))
            .handle((BackendMessage message, SynchronousSink<BackendMessage> sink) -> {

                if (message instanceof CommandComplete) {
                    requests.emitNext(new Close(portal, PORTAL), Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.next(message);
                } else if (message instanceof ErrorResponse) {
                    requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    sink.next(message);
                } else if (message instanceof PortalSuspended) {
                    if (isCanceled.get()) {
                        requests.emitNext(new Close(portal, PORTAL), Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitNext(Sync.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    } else {
                        requests.emitNext(new Execute(portal, fetchSize), Sinks.EmitFailureHandler.FAIL_FAST);
                        requests.emitNext(Flush.INSTANCE, Sinks.EmitFailureHandler.FAIL_FAST);
                    }
                } else {
                    sink.next(message);
                }
            }).doFinally(ignore -> operator.close(requests))
            .as(flux -> Operators.discardOnCancel(flux, () -> isCanceled.set(true)));
    }

    private static BiConsumer<BackendMessage, SynchronousSink<BackendMessage>> handleReprepare(Sinks.Many<FrontendMessage> requests, ExtendedFlowOperator operator, MessageFactory messageFactory) {
        AtomicBoolean reprepared = new AtomicBoolean();

        return (message, sink) -> {

            if (message instanceof ErrorResponse && requiresReprepare((ErrorResponse) message)) {

                operator.evictCachedStatement();

                if (reprepared.compareAndSet(false, true)) {

                    List<FrontendMessage.DirectEncoder> messages = messageFactory.createMessages();
                    if (!messages.contains(Sync.INSTANCE)) {
                        messages.add(0, Sync.INSTANCE);
                    }
                    requests.emitNext(new CompositeFrontendMessage(messages), Sinks.EmitFailureHandler.FAIL_FAST);

                    return;
                }
            }

            sink.next(message);
        };
    }

    private static boolean requiresReprepare(ErrorResponse errorResponse) {
        ErrorDetails details = new ErrorDetails(errorResponse.getFields());
        String code = details.getCode();

        // "prepared statement \"S_2\" does not exist"
        // INVALID_SQL_STATEMENT_NAME
        if ("26000".equals(code)) {
            return true;
        }
        // NOT_IMPLEMENTED

        if (!"0A000".equals(code)) {
            return false;
        }

        String routine = details.getRoutine().orElse(null);
        // "cached plan must not change result type"
        return "RevalidateCachedQuery".equals(routine) // 9.2+
            || "RevalidateCachedPlan".equals(routine); // <= 9.1
    }

    interface MessageFactory {

        List<FrontendMessage.DirectEncoder> createMessages();

    }

    /**
     * Operator to encapsulate common activity around the extended flow. Subclasses {@link AtomicInteger} to capture the number of ReadyForQuery frames.
     */
    static class ExtendedFlowOperator extends AtomicInteger implements Predicate<BackendMessage> {

        private final String sql;

        private final Binding binding;

        private volatile @Nullable String name;

        private final StatementCache cache;

        private final List<ByteBuf> values;

        private final String portal;

        private final boolean forceBinary;

        public ExtendedFlowOperator(String sql, Binding binding, StatementCache cache, List<ByteBuf> values, String portal, boolean forceBinary) {
            this.sql = sql;
            this.binding = binding;
            this.cache = cache;
            this.values = values;
            this.portal = portal;
            this.forceBinary = forceBinary;
        }

        public void close(Sinks.Many<FrontendMessage> requests) {
            requests.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            this.values.forEach(ReferenceCountUtil::release);
        }

        public void evictCachedStatement() {
            synchronized (this) {
                this.name = null;
            }
            this.cache.evict(this.sql);
        }

        public void hydrateStatementCache() {
            this.cache.put(this.binding, this.sql, getStatementName());
        }

        public Predicate<BackendMessage> takeUntil() {
            return this;
        }

        @Override
        public boolean test(BackendMessage backendMessage) {
            if (backendMessage instanceof ReadyForQuery) {
                return decrementAndGet() <= 0;
            }

            return false;
        }

        private boolean isPrepareRequired() {
            return this.cache.requiresPrepare(this.binding, this.sql);
        }

        public String getStatementName() {
            synchronized (this) {

                if (this.name == null) {
                    this.name = this.cache.getName(this.binding, this.sql);
                }
                return this.name;
            }
        }

        public List<FrontendMessage.DirectEncoder> getMessages(Collection<FrontendMessage.DirectEncoder> append) {
            incrementAndGet();
            List<FrontendMessage.DirectEncoder> messagesToSend = new ArrayList<>(6);

            if (isPrepareRequired()) {
                messagesToSend.add(new Parse(getStatementName(), this.binding.getParameterTypes(), this.sql));
            }

            for (ByteBuf value : this.values) {
                value.readerIndex(0);
                value.touch("ExtendedFlowOperator").retain();
            }

            Bind bind = new Bind(this.portal, this.binding.getParameterFormats(), this.values, ExtendedQueryMessageFlow.resultFormat(this.forceBinary), getStatementName());

            messagesToSend.add(bind);
            messagesToSend.add(new Describe(this.portal, PORTAL));
            messagesToSend.addAll(append);

            return messagesToSend;
        }

    }

}
