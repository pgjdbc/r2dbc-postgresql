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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FluxDiscardOnCancel}.
 */
final class FluxDiscardOnCancelUnitTests {

    @Test
    void shouldEmitAllItemsOnSubscription() {

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(Operators::discardOnCancel)
            .as(StepVerifier::create)
            .expectNext(0, 1, 2, 3)
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void considersAssemblyHook() {

        List<Object> publishers = new ArrayList<>();
        Hooks.onEachOperator(objectPublisher -> {
            publishers.add(objectPublisher);

            return objectPublisher;
        });

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .transform(Operators::discardOnCancel)
            .as(StepVerifier::create)
            .expectNextCount(4)
            .verifyComplete();

        assertThat(publishers).hasSize(2).extracting(Object::getClass).contains((Class) FluxDiscardOnCancel.class);
    }

    @Test
    void considersOnDropHook() {

        List<Object> discard = new ArrayList<>();

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(Operators::discardOnCancel)
            .doOnDiscard(Object.class, discard::add)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(discard).containsOnly(2, 3);
    }

    @Test
    void considersCancelSignalPropagation() {

        AtomicBoolean cancelled = new AtomicBoolean();

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(it -> Operators.discardOnCancel(it, () -> cancelled.set(true)))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(cancelled).isTrue();
    }

    @Test
    void shouldNotConsumeItemsOnCancel() {

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(items).toIterable().containsSequence(2, 3);
    }

    @Test
    void shouldConsumeAndDiscardItemsOnCancel() {

        Iterator<Integer> items = createItems(4);

        Flux.fromIterable(() -> items)
            .as(Operators::discardOnCancel)
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(2)
            .expectNext(0, 1)
            .thenCancel()
            .verify();

        assertThat(items).toIterable().isEmpty();
    }

    static Iterator<Integer> createItems(int count) {
        return IntStream.range(0, count).boxed().iterator();
    }
}
