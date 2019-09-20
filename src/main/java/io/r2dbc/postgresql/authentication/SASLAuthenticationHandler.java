package io.r2dbc.postgresql.authentication;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.exception.ScramInvalidServerSignatureException;
import com.ongres.scram.common.exception.ScramParseException;
import com.ongres.scram.common.exception.ScramServerErrorException;
import io.r2dbc.postgresql.message.backend.AuthenticationMessage;
import io.r2dbc.postgresql.message.backend.AuthenticationSASL;
import io.r2dbc.postgresql.message.backend.AuthenticationSASLContinue;
import io.r2dbc.postgresql.message.backend.AuthenticationSASLFinal;
import io.r2dbc.postgresql.message.frontend.FrontendMessage;
import io.r2dbc.postgresql.message.frontend.SASLInitialResponse;
import io.r2dbc.postgresql.message.frontend.SASLResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufferUtils;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;

import static com.ongres.scram.client.ScramClient.ChannelBinding.NO;
import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;

public class SASLAuthenticationHandler implements AuthenticationHandler {

    private final CharSequence password;

    private final String username;

    private ScramSession.ClientFinalProcessor clientFinalProcessor;

    private ScramSession scramSession;

    /**
     * Creates a new handler.
     *
     * @param password the password to use for authentication
     * @param username the username to use for authentication
     * @throws IllegalArgumentException if {@code password} or {@code user} is {@code null}
     */
    public SASLAuthenticationHandler(CharSequence password, String username) {
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.username = Assert.requireNonNull(username, "username must not be null");
    }


    /**
     * Returns whether this {@link AuthenticationHandler} can support authentication for a given authentication message response.
     *
     * @param message the message to inspect
     * @return whether this {@link AuthenticationHandler} can support authentication for a given authentication message response
     * @throws IllegalArgumentException if {@code message} is {@code null}
     */
    public static boolean supports(AuthenticationMessage message) {
        Assert.requireNonNull(message, "message must not be null");

        return message instanceof AuthenticationSASL || message instanceof AuthenticationSASLContinue || message instanceof AuthenticationSASLFinal;
    }

    @Override
    public FrontendMessage handle(AuthenticationMessage message) {
        if (message instanceof AuthenticationSASL) {
            return handleAuthenticationSASL((AuthenticationSASL) message);
        }

        if (message instanceof AuthenticationSASLContinue) {
            return handleAuthenticationSASLContinue((AuthenticationSASLContinue) message);
        }

        if (message instanceof AuthenticationSASLFinal) {
            return handleAuthenticationSASLFinal((AuthenticationSASLFinal) message);
        }

        throw new IllegalArgumentException(String.format("Cannot handle %s message", message.getClass().getSimpleName()));
    }

    private FrontendMessage handleAuthenticationSASL(AuthenticationSASL message) {
        ScramClient scramClient = ScramClient
            .channelBinding(NO)
            .stringPreparation(NO_PREPARATION)
            .selectMechanismBasedOnServerAdvertised(message.getAuthenticationMechanisms().toArray(new String[0]))
            .setup();

        this.scramSession = scramClient.scramSession(this.username);

        return new SASLInitialResponse(ByteBufferUtils.encode(this.scramSession.clientFirstMessage()), scramClient.getScramMechanism().getName());
    }

    private FrontendMessage handleAuthenticationSASLContinue(AuthenticationSASLContinue message) {
        try {
            this.clientFinalProcessor = this.scramSession
                .receiveServerFirstMessage(ByteBufferUtils.decode(message.getData()))
                .clientFinalProcessor(this.password.toString());

            return new SASLResponse(ByteBufferUtils.encode(clientFinalProcessor.clientFinalMessage()));
        } catch (ScramParseException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Nullable
    private FrontendMessage handleAuthenticationSASLFinal(AuthenticationSASLFinal message) {
        try {
            this.clientFinalProcessor.receiveServerFinalMessage(ByteBufferUtils.decode(message.getAdditionalData()));
            return null;
        } catch (ScramParseException | ScramInvalidServerSignatureException | ScramServerErrorException e) {
            throw Exceptions.propagate(e);
        }
    }

}
