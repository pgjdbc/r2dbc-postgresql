/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.postgresql.util;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A decorating operator that replays signals from its source to a {@link Subscriber} and drains the source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without any previous
 * response state.
 */
class FluxDiscardOnCancel<T> extends FluxOperator<T, T> {

    private static final Logger logger = Loggers.getLogger(FluxDiscardOnCancel.class);

    private final Runnable cancelConsumer;

    FluxDiscardOnCancel(Flux<? extends T> source, Runnable cancelConsumer) {
        super(source);
        this.cancelConsumer = cancelConsumer;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(new FluxDiscardOnCancelSubscriber<>(actual, this.cancelConsumer));
    }

    static class FluxDiscardOnCancelSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Subscription {

        final CoreSubscriber<T> actual;

        final Context ctx;

        final Runnable cancelConsumer;

        Subscription s;

        FluxDiscardOnCancelSubscriber(CoreSubscriber<T> actual, Runnable cancelConsumer) {

            this.actual = actual;
            this.ctx = actual.currentContext();
            this.cancelConsumer = cancelConsumer;
        }

        @Override
        public void onSubscribe(Subscription s) {

            if (Operators.validate(this.s, s)) {
                this.s = s;
                this.actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {

            if (this.get()) {
                Operators.onDiscard(t, this.ctx);
                return;
            }

            this.actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (!this.get()) {
                this.actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (!this.get()) {
                this.actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            this.s.request(n);
        }

        @Override
        public void cancel() {

            if (compareAndSet(false, true)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("received cancel signal");
                }
                try {
                    this.cancelConsumer.run();
                } catch (Exception e) {
                    Operators.onErrorDropped(e, this.ctx);
                }
                this.s.request(Long.MAX_VALUE);
            }
        }

    }

}
