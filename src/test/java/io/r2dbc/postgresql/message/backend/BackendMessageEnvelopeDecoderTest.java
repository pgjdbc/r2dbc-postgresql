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

package io.r2dbc.postgresql.message.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCounted;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Consumer;
import java.util.function.Function;

import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

final class BackendMessageEnvelopeDecoderTest {

    @Test
    void testEmptyBuf() {
        testSplit(
            StepVerifier.LastStep::verifyComplete,
            TEST.buffer());
    }

    @Test
    void testCompleteEnvelope() {
        testSplit(
            s -> s
                .expectNextCount(1)
                .verifyComplete(),
            envelope('R', buffer -> buffer.writeInt(3)));
    }

    @Test
    void testMultipleEnvelopes() {
        ByteBuf envelope = envelope('R', buffer -> buffer.writeInt(3));
        CompositeByteBuf buf = TEST.compositeBuffer();
        buf.writeBytes(envelope.slice());
        buf.writeBytes(envelope.slice());
        envelope.release();

        testSplit(
            s -> s
                .expectNextCount(2)
                .verifyComplete(),
            buf);
    }

    @Test
    void testPartialEnvelopes() {
        ByteBuf envelope = envelope('R', buffer -> buffer.writeInt(3));
        ByteBuf part1 = envelope.copy(0, 2);
        ByteBuf part2 = envelope.copy(2, envelope.readableBytes() - 2)
            .writeBytes(envelope.copy(0, 2));
        ByteBuf part3 = envelope.copy(2, envelope.readableBytes() - 2);
        envelope.release();

        testSplit(
            s -> s
                .expectNextCount(2)
                .verifyComplete(),
            part1, part2, part3);

    }

    @Test
    void testDisposeWhileSplitting() {
        ByteBuf envelope = envelope('R', buffer -> buffer.writeInt(3));
        CompositeByteBuf buf = TEST.compositeBuffer()
            .addComponent(true, envelope.copy())
            .addComponent(true, envelope.copy());

        BackendMessageEnvelopeDecoder splitter = new BackendMessageEnvelopeDecoder(TEST);
        splitter.apply(buf)
            .doOnNext(next -> splitter.dispose())
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    private ByteBuf envelope(char discriminator, Function<ByteBuf, ByteBuf> payloadWriter) {
        ByteBuf payload = payloadWriter.apply(TEST.buffer());
        ByteBuf envelope = TEST.buffer(5 + payload.readableBytes())
            .writeByte(discriminator)
            .writeInt(4 + payload.readableBytes())
            .writeBytes(payload);
        payload.release();
        return envelope;
    }

    private void testSplit(Consumer<StepVerifier.FirstStep<CompositeByteBuf>> stepConsumer, ByteBuf... bufs) {
        BackendMessageEnvelopeDecoder splitter = new BackendMessageEnvelopeDecoder(TEST);

        stepConsumer.accept(Flux.just(bufs)
            .concatMap(splitter)
            .doOnNext(c -> assertThat(c.refCnt()).isOne())
            .doOnNext(ReferenceCounted::release)
            .as(StepVerifier::create));
        splitter.dispose();
        for (ByteBuf buf : bufs) {
            assertThat(buf.refCnt()).isZero();
        }
    }
}
