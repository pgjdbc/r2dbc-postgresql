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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.function.Function;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.backend.Field.FieldType.CODE;
import static io.r2dbc.postgresql.message.backend.ReadyForQuery.TransactionStatus.IDLE;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

final class BackendMessageDecoderTest {

    @Test
    void authenticationCleartextPassword() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(3));
        assertThat(message).isEqualTo(AuthenticationCleartextPassword.INSTANCE);
    }

    @Test
    void authenticationGSS() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(7));
        assertThat(message).isEqualTo(AuthenticationGSS.INSTANCE);
    }

    @Test
    void authenticationGSSContinue() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(8).writeInt(100));
        assertThat(message).isEqualTo(new AuthenticationGSSContinue(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void authenticationKerberosV5() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(2));
        assertThat(message).isEqualTo(AuthenticationKerberosV5.INSTANCE);
    }

    @Test
    void authenticationMD5Password() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(5).writeInt(100));
        assertThat(message).isEqualTo(new AuthenticationMD5Password(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void authenticationOk() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(0));
        assertThat(message).isEqualTo(AuthenticationOk.INSTANCE);
    }

    @Test
    void authenticationSASL() {
        BackendMessage message = decode('R', buffer -> {
            buffer.writeInt(10);

            buffer.writeCharSequence("test-authentication-mechanism", UTF_8);
            buffer.writeByte(0);

            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new AuthenticationSASL(Collections.singletonList("test-authentication-mechanism")));
    }

    @Test
    void authenticationSASLContinue() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(11).writeInt(100));
        assertThat(message).isEqualTo(new AuthenticationSASLContinue(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void authenticationSASLFinal() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(12).writeInt(100));
        assertThat(message).isEqualTo(new AuthenticationSASLFinal(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void authenticationSCMCredential() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(6));
        assertThat(message).isEqualTo(AuthenticationSCMCredential.INSTANCE);
    }

    @Test
    void authenticationSSPI() {
        BackendMessage message = decode('R', buffer -> buffer.writeInt(9));
        assertThat(message).isEqualTo(AuthenticationSSPI.INSTANCE);
    }

    @Test
    void backendKeyData() {
        BackendMessage message = decode('K', buffer -> buffer.writeInt(100).writeInt(200));
        assertThat(message).isEqualTo(new BackendKeyData(100, 200));
    }

    @Test
    void bindComplete() {
        BackendMessage message = decode('2', buffer -> buffer);
        assertThat(message).isEqualTo(BindComplete.INSTANCE);
    }

    @Test
    void closeComplete() {
        BackendMessage message = decode('3', buffer -> buffer);
        assertThat(message).isEqualTo(CloseComplete.INSTANCE);
    }

    @Test
    void commandComplete() {
        BackendMessage message = decode('C', buffer -> {
            buffer.writeCharSequence("COPY 100", UTF_8);
            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new CommandComplete("COPY", null, 100));
    }

    @Test
    void copyBothResponse() {
        BackendMessage message = decode('W', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1));
        assertThat(message).isEqualTo(new CopyBothResponse(EnumSet.of(FORMAT_BINARY), FORMAT_BINARY));
    }

    @Test
    void copyData() {
        BackendMessage message = decode('d', buffer -> buffer.writeInt(100));
        assertThat(message).isEqualTo(new CopyData(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void copyDone() {
        BackendMessage message = decode('c', buffer -> buffer);
        assertThat(message).isEqualTo(CopyDone.INSTANCE);
    }

    @Test
    void copyInResponse() {
        BackendMessage message = decode('G', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1));
        assertThat(message).isEqualTo(new CopyInResponse(EnumSet.of(FORMAT_BINARY), FORMAT_BINARY));
    }

    @Test
    void copyOutResponse() {
        BackendMessage message = decode('H', buffer -> buffer
            .writeByte(1)
            .writeShort(1)
            .writeShort(1));
        assertThat(message).isEqualTo(new CopyOutResponse(EnumSet.of(FORMAT_BINARY), FORMAT_BINARY));
    }

    @Test
    void dataRow() {
        DataRow message = (DataRow) decode('D', buffer -> buffer
            .writeShort(1)
            .writeInt(4)
            .writeInt(100));
        assertThat(message).isEqualTo(new DataRow(TEST.buffer(4).writeInt(100)));
        message.release();
    }

    @Test
    void emptyQueryResponse() {
        BackendMessage message = decode('I', buffer -> buffer);
        assertThat(message).isEqualTo(EmptyQueryResponse.INSTANCE);
    }

    @Test
    void errorResponse() {
        BackendMessage message = decode('E', buffer -> {
            buffer.writeByte('C');

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new ErrorResponse(Collections.singletonList(new Field(CODE, "test-value"))));
    }

    @Test
    void functionCallResponse() {
        BackendMessage message = decode('V', buffer -> buffer.writeInt(4).writeInt(100));
        assertThat(message).isEqualTo(new FunctionCallResponse(TEST.buffer(4).writeInt(100)));
    }

    @Test
    void invalidAuthenticationMessageType() {
        try {
            decode('R', buffer -> buffer.writeInt(100));
            fail("Exception expected");
        } catch (Exception error) {
            assertThat(error).isInstanceOf(IllegalArgumentException.class).hasMessage("100 is not a valid authentication type");
        }
    }

    @Test
    void invalidMessageType() {
        try {
            decode('Q', buffer -> buffer);
            fail("Exception expected");
        } catch (Exception error) {
            assertThat(error).isInstanceOf(IllegalArgumentException.class).hasMessage("Q is not a valid message type");
        }
    }

    @Test
    void noticeResponse() {
        BackendMessage message = decode('N', buffer -> {
            buffer.writeByte('C');

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new NoticeResponse(Collections.singletonList(new Field(CODE, "test-value"))));
    }

    @Test
    void notificationResponse() {
        BackendMessage message = decode('A', buffer -> {
            buffer.writeInt(100);

            buffer.writeCharSequence("test-name", UTF_8);
            buffer.writeByte(0);

            buffer.writeCharSequence("test-payload", UTF_8);
            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new NotificationResponse("test-name", "test-payload", 100));
    }

    @Test
    void parameterDescription() {
        BackendMessage message = decode('t', buffer -> buffer
            .writeShort(1)
            .writeInt(100));
        assertThat(message).isEqualTo(new ParameterDescription(Collections.singletonList(100)));
    }

    @Test
    void parameterStatus() {
        BackendMessage message = decode('S', buffer -> {
            buffer.writeCharSequence("test-name", UTF_8);
            buffer.writeByte(0);

            buffer.writeCharSequence("test-value", UTF_8);
            buffer.writeByte(0);

            return buffer;
        });
        assertThat(message).isEqualTo(new ParameterStatus("test-name", "test-value"));
    }

    @Test
    void parseComplete() {
        BackendMessage message = decode('1', buffer -> buffer);
        assertThat(message).isEqualTo(ParseComplete.INSTANCE);
    }

    @Test
    void portalSuspended() {
        BackendMessage message = decode('s', buffer -> buffer);
        assertThat(message).isEqualTo(PortalSuspended.INSTANCE);
    }

    @Test
    void readyForQuery() {
        BackendMessage message = decode('Z', buffer -> buffer.writeByte('I'));
        assertThat(message).isEqualTo(new ReadyForQuery(IDLE));
    }

    @Test
    void rowDescription() {
        BackendMessage message = decode('T', buffer -> {
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
        });
        assertThat(message).isEqualTo(new RowDescription(Collections.singletonList(new RowDescription.Field((short) 100, 200, 300, (short) 400, FORMAT_BINARY, "test-name", 500))));
    }

    private BackendMessage decode(char discriminator, Function<ByteBuf, ByteBuf> decode) {
        CompositeByteBuf data = TEST.compositeBuffer();
        ByteBuf payload = decode.apply(TEST.buffer());
        ByteBuf envelope = TEST.buffer(5 + payload.readableBytes())
            .writeByte(discriminator)
            .writeInt(4 + payload.readableBytes())
            .writeBytes(payload);
        payload.release();
        data.addComponent(true, envelope);
        return BackendMessageDecoder.decode(data);
    }

}
