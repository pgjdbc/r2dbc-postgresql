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
import io.r2dbc.postgresql.util.Assert;
import reactor.core.publisher.Flux;

import static io.r2dbc.postgresql.message.backend.BackendMessageUtils.getBody;

/**
 * A decoder that reads {@link ByteBuf}s and returns a {@link Flux} of decoded {@link BackendMessage}s.
 */
public final class BackendMessageDecoder {

    /**
     * Decode a {@link ByteBuf} into a {@link BackendMessage}.
     *
     * @param envelope the {@link ByteBuf} to decode
     * @return a {@link Flux} of {@link BackendMessage}s
     */
    public static BackendMessage decode(CompositeByteBuf envelope) {
        Assert.requireNonNull(envelope, "in must not be null");

        try {
            MessageType messageType = MessageType.valueOf(envelope.readByte());
            ByteBuf body = getBody(envelope);
            return decodeBody(body, messageType);
        } finally {
            envelope.release();
        }
    }

    private static BackendMessage decodeBody(ByteBuf body, MessageType messageType) {
        switch (messageType) {
            case AUTHENTICATION:
                return decodeAuthentication(body);
            case BACKEND_KEY_DATA:
                return BackendKeyData.decode(body);
            case BIND_COMPLETE:
                return BindComplete.INSTANCE;
            case CLOSE_COMPLETE:
                return CloseComplete.INSTANCE;
            case COMMAND_COMPLETE:
                return CommandComplete.decode(body);
            case COPY_DATA:
                return CopyData.decode(body);
            case COPY_DONE:
                return CopyDone.INSTANCE;
            case COPY_BOTH_RESPONSE:
                return CopyBothResponse.decode(body);
            case COPY_IN_RESPONSE:
                return CopyInResponse.decode(body);
            case COPY_OUT_RESPONSE:
                return CopyOutResponse.decode(body);
            case DATA_ROW:
                return DataRow.decode(body);
            case EMPTY_QUERY_RESPONSE:
                return EmptyQueryResponse.INSTANCE;
            case ERROR_RESPONSE:
                return ErrorResponse.decode(body);
            case FUNCTION_CALL_RESPONSE:
                return FunctionCallResponse.decode(body);
            case NO_DATA:
                return NoData.INSTANCE;
            case NOTICE_RESPONSE:
                return NoticeResponse.decode(body);
            case NOTIFICATION_RESPONSE:
                return NotificationResponse.decode(body);
            case PARAMETER_DESCRIPTION:
                return ParameterDescription.decode(body);
            case PARAMETER_STATUS:
                return ParameterStatus.decode(body);
            case PARSE_COMPLETE:
                return ParseComplete.INSTANCE;
            case PORTAL_SUSPENDED:
                return PortalSuspended.INSTANCE;
            case READY_FOR_QUERY:
                return ReadyForQuery.decode(body);
            case ROW_DESCRIPTION:
                return RowDescription.decode(body);
            default:
                throw new IllegalArgumentException(String.format("%s is not a supported message type", messageType));
        }
    }

    private static BackendMessage decodeAuthentication(ByteBuf in) {
        AuthenticationType authenticationType = AuthenticationType.valueOf(in.readInt());

        switch (authenticationType) {
            case OK:
                return AuthenticationOk.INSTANCE;
            case KERBEROS_V5:
                return AuthenticationKerberosV5.INSTANCE;
            case CLEARTEXT_PASSWORD:
                return AuthenticationCleartextPassword.INSTANCE;
            case GSS:
                return AuthenticationGSS.INSTANCE;
            case GSS_CONTINUE:
                return AuthenticationGSSContinue.decode(in);
            case MD5_PASSWORD:
                return AuthenticationMD5Password.decode(in);
            case SCMC_CREDENTIAL:
                return AuthenticationSCMCredential.INSTANCE;
            case SASL:
                return AuthenticationSASL.decode(in);
            case SASL_CONTINUE:
                return AuthenticationSASLContinue.decode(in);
            case SASL_FINAL:
                return AuthenticationSASLFinal.decode(in);
            case SSPI:
                return AuthenticationSSPI.INSTANCE;
            default:
                throw new IllegalArgumentException(String.format("%s is not a supported authentication type", authenticationType));
        }
    }

    private enum AuthenticationType {

        OK(0),
        KERBEROS_V5(2),
        CLEARTEXT_PASSWORD(3),
        GSS(7),
        GSS_CONTINUE(8),
        MD5_PASSWORD(5),
        SCMC_CREDENTIAL(6),
        SASL(10),
        SASL_CONTINUE(11),
        SASL_FINAL(12),
        SSPI(9);

        private final int discriminator;

        AuthenticationType(int discriminator) {
            this.discriminator = discriminator;
        }

        static AuthenticationType valueOf(int i) {
            for (AuthenticationType authType : values()) {
                if (authType.discriminator == i) {
                    return authType;
                }
            }
            throw new IllegalArgumentException(String.format("%d is not a valid authentication type", i));
        }

    }

    private enum MessageType {

        AUTHENTICATION('R'),
        BACKEND_KEY_DATA('K'),
        BIND_COMPLETE('2'),
        CLOSE_COMPLETE('3'),
        COMMAND_COMPLETE('C'),
        COPY_BOTH_RESPONSE('W'),
        COPY_DATA('d'),
        COPY_DONE('c'),
        COPY_IN_RESPONSE('G'),
        COPY_OUT_RESPONSE('H'),
        DATA_ROW('D'),
        EMPTY_QUERY_RESPONSE('I'),
        ERROR_RESPONSE('E'),
        FUNCTION_CALL_RESPONSE('V'),
        NO_DATA('n'),
        NOTICE_RESPONSE('N'),
        NOTIFICATION_RESPONSE('A'),
        PARAMETER_DESCRIPTION('t'),
        PARAMETER_STATUS('S'),
        PARSE_COMPLETE('1'),
        PORTAL_SUSPENDED('s'),
        READY_FOR_QUERY('Z'),
        ROW_DESCRIPTION('T');

        private final char discriminator;

        MessageType(char discriminator) {
            this.discriminator = discriminator;
        }

        static MessageType valueOf(byte b) {
            for (MessageType messageType : values()) {
                if (messageType.discriminator == b) {
                    return messageType;
                }
            }
            throw new IllegalArgumentException(String.format("%c is not a valid message type", b));
        }

    }

}
