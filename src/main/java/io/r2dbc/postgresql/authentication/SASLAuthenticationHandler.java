package io.r2dbc.postgresql.authentication;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.StringPreparation;
import com.ongres.scram.common.exception.ScramException;

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

public class SASLAuthenticationHandler implements AuthenticationHandler {

    private final CharSequence password;

    private final String username;

    private ScramClient scramClient;

    /**
     * Create a new handler.
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
        this.scramClient = ScramClient.builder()
            .advertisedMechanisms(message.getAuthenticationMechanisms())
            .username(username) // ignored by the server, use startup message
            .password(password.toString().toCharArray())
            .stringPreparation(StringPreparation.POSTGRESQL_PREPARATION)
            .build();

        return new SASLInitialResponse(ByteBufferUtils.encode(this.scramClient.clientFirstMessage().toString()), scramClient.getScramMechanism().getName());
    }

    private FrontendMessage handleAuthenticationSASLContinue(AuthenticationSASLContinue message) {
        try {
            this.scramClient.serverFirstMessage(ByteBufferUtils.decode(message.getData()));

            return new SASLResponse(ByteBufferUtils.encode(this.scramClient.clientFinalMessage().toString()));
        } catch (ScramException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Nullable
    private FrontendMessage handleAuthenticationSASLFinal(AuthenticationSASLFinal message) {
        try {
            this.scramClient.serverFinalMessage(ByteBufferUtils.decode(message.getAdditionalData()));
            return null;
        } catch (ScramException e) {
            throw Exceptions.propagate(e);
        }
    }

}
