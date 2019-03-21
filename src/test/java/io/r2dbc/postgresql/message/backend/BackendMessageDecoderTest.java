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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.FirstStep;

import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;
import static io.r2dbc.postgresql.message.backend.ReadyForQuery.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;

final class BackendMessageDecoderTest {

    @Test
    void authenticationCleartextPassword() {
        decode('R', buffer -> buffer.writeInt(3))
            .expectNext(AuthenticationCleartextPassword.INSTANCE)
            .verifyComplete();
    }

    @Test
    void authenticationGSS() {
        decode('R', buffer -> buffer.writeInt(7))
            .expectNext(AuthenticationGSS.INSTANCE)
            .verifyComplete();
    }

    @Test
    void authenticationGSSContinue() {
        decode('R', buffer -> buffer.writeInt(8).writeInt(100))
            .expectNext(new AuthenticationGSSContinue(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void authenticationKerberosV5() {
        decode('R', buffer -> buffer.writeInt(2))
            .expectNext(AuthenticationKerberosV5.INSTANCE)
            .verifyComplete();
    }

    @Test
    void authenticationMD5Password() {
        decode('R', buffer -> buffer.writeInt(5).writeInt(100))
            .expectNext(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void authenticationOk() {
        decode('R', buffer -> buffer.writeInt(0))
            .expectNext(AuthenticationOk.INSTANCE)
            .verifyComplete();
    }

    @Test
    void authenticationSASL() {
        decode('R', buffer -> {
            buffer.writeInt(10);

            buffer.writeCharSequence("test-authentication-mechanism", UTF_8);
            buffer.writeByte(0);

            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new AuthenticationSASL(Collections.singletonList("test-authentication-mechanism")))
            .verifyComplete();
    }

    @Test
    void authenticationSASLContinue() {
        decode('R', buffer -> buffer.writeInt(11).writeInt(100))
            .expectNext(new AuthenticationSASLContinue(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void authenticationSASLFinal() {
        decode('R', buffer -> buffer.writeInt(12).writeInt(100))
            .expectNext(new AuthenticationSASLFinal(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void authenticationSCMCredential() {
        decode('R', buffer -> buffer.writeInt(6))
            .expectNext(AuthenticationSCMCredential.INSTANCE)
            .verifyComplete();
    }

    @Test
    void authenticationSSPI() {
        decode('R', buffer -> buffer.writeInt(9))
            .expectNext(AuthenticationSSPI.INSTANCE)
            .verifyComplete();
    }

    @Test
    void backendKeyData() {
        decode('K', buffer -> buffer.writeInt(100).writeInt(200))
            .expectNext(new BackendKeyData(100, 200))
            .verifyComplete();
    }

    @Test
    void bindComplete() {
        decode('2', buffer -> buffer)
            .expectNext(BindComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    void closeComplete() {
        decode('3', buffer -> buffer)
            .expectNext(CloseComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    void commandComplete() {
        decode('C', buffer -> {
            buffer.writeCharSequence("COPY 100", UTF_8);
            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new CommandComplete("COPY", null, 100))
            .verifyComplete();
    }

    @Test
    void copyBothResponse() {
        decode('W', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1))
            .expectNext(new CopyBothResponse(Collections.singletonList(FORMAT_BINARY), FORMAT_BINARY))
            .verifyComplete();
    }

    @Test
    void copyData() {
        decode('d', buffer -> buffer.writeInt(100))
            .expectNext(new CopyData(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void copyDone() {
        decode('c', buffer -> buffer)
            .expectNext(CopyDone.INSTANCE)
            .verifyComplete();
    }

    @Test
    void copyInResponse() {
        decode('G', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1))
            .expectNext(new CopyInResponse(Collections.singletonList(FORMAT_BINARY), FORMAT_BINARY))
            .verifyComplete();
    }

    @Test
    void copyOutResponse() {
        decode('H', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1))
            .expectNext(new CopyOutResponse(Collections.singletonList(FORMAT_BINARY), FORMAT_BINARY))
            .verifyComplete();
    }

    @Test
    void dataRow() {
        decodeWithRelease('D', message -> ((DataRow) message).release(), buffer -> buffer
            .writeShort(1)
            .writeInt(4)
            .writeInt(100))
            .expectNext(new DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))))
            .verifyComplete();
    }

    @Test
    void emptyQueryResponse() {
        decode('I', buffer -> buffer)
            .expectNext(EmptyQueryResponse.INSTANCE)
            .verifyComplete();
    }

    @Test
    void errorResponse() {
        decode('E', buffer -> {
            buffer.writeByte('C');

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new ErrorResponse(Collections.singletonList(new Field(CODE, "test-value"))))
            .verifyComplete();
    }

    @Test
    void functionCallResponse() {
        decode('V', buffer -> buffer.writeInt(4).writeInt(100))
            .expectNext(new FunctionCallResponse(TEST.buffer(4).writeInt(100)))
            .verifyComplete();
    }

    @Test
    void invalidAuthenticationMessageType() {
        decode('R', buffer -> buffer
            .writeInt(100))
            .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalArgumentException.class).hasMessage("100 is not a valid authentication type"))
            .verify();
    }

    @Test
    void invalidMessageType() {
        decode('Q', buffer -> buffer)
            .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(IllegalArgumentException.class).hasMessage("Q is not a valid message type"))
            .verify();
    }

    @Test
    void multiple() {
        decode('R', buffer -> buffer.writeInt(0), buffer -> buffer.writeInt(0))
            .expectNext(AuthenticationOk.INSTANCE)
            .expectNext(AuthenticationOk.INSTANCE)
            .verifyComplete();
    }

    @Test
    void noData() {
        decode('n', buffer -> buffer)
            .expectNext(NoData.INSTANCE)
            .verifyComplete();
    }

    @Test
    void noticeResponse() {
        decode('N', buffer -> {
            buffer.writeByte('C');

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new NoticeResponse(Collections.singletonList(new Field(CODE, "test-value"))))
            .verifyComplete();
    }

    @Test
    void notificationResponse() {
        decode('A', buffer -> {
            buffer.writeInt(100);

            buffer.writeCharSequence("test-name", UTF_8);
            buffer.writeByte(0);

            buffer.writeCharSequence("test-payload", UTF_8);
            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new NotificationResponse("test-name", "test-payload", 100))
            .verifyComplete();
    }

    @Test
    void parameterDescription() {
        decode('t', buffer -> buffer
            .writeShort(1)
            .writeInt(100))
            .expectNext(new ParameterDescription(Collections.singletonList(100)))
            .verifyComplete();
    }

    @Test
    void parameterStatus() {
        decode('S', buffer -> {
            buffer.writeCharSequence("test-name", UTF_8);
            buffer.writeByte(0);

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        })
            .expectNext(new ParameterStatus("test-name", "test-value"))
            .verifyComplete();
    }

    @Test
    void parseComplete() {
        decode('1', buffer -> buffer)
            .expectNext(ParseComplete.INSTANCE)
            .verifyComplete();
    }

    @Test
    void portalSuspended() {
        decode('s', buffer -> buffer)
            .expectNext(PortalSuspended.INSTANCE)
            .verifyComplete();
    }

    @Test
    void readyForQuery() {
        decode('Z', buffer -> buffer.writeByte('I'))
            .expectNext(new ReadyForQuery(IDLE))
            .verifyComplete();
    }

    @Test
    void rowDescription() {
        decode('T', buffer -> {
            buffer.writeShort(1);

            buffer.writeCharSequence("test-name", UTF_8);
            buffer.writeByte(0);

            buffer
                .writeInt(500)
                .writeShort(100)
                .writeInt(200)
                .writeShort(400)
                .writeInt(300)
                .writeShort(1);

            return buffer;
        })
            .expectNext(new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_BINARY, "test-name", 500))))
            .verifyComplete();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final FirstStep<BackendMessage> decode(char discriminator, Function<ByteBuf, ByteBuf>... decodes) {
        return decodeWithRelease(discriminator, message -> {
        }, decodes);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final FirstStep<BackendMessage> decodeWithRelease(char discriminator, Consumer<BackendMessage> release, Function<ByteBuf, ByteBuf>... decodes) {
        ByteBuf data = Stream.of(decodes)
            .map(decode -> decode.apply(TEST.buffer()))
            .map(payload ->
                TEST.buffer(5 + payload.readableBytes())
                    .writeByte(discriminator)
                    .writeInt(4 + payload.readableBytes())
                    .writeBytes(payload))
            .reduce(TEST.buffer(), ByteBuf::writeBytes);

        BackendMessageDecoder decoder = new BackendMessageDecoder(TEST);

        return Flux.just(data.readRetainedSlice(data.readableBytes() / 2), data)
            .concatMap(decoder::decode)
            .doOnNext(release)
            .doAfterTerminate(() -> assertThat(data.refCnt()).isZero())
            .as(StepVerifier::create);
    }

}
