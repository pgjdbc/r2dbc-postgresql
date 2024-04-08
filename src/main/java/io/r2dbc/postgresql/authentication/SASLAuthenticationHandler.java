package io.r2dbc.postgresql.authentication;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.StringPreparation;
import com.ongres.scram.common.exception.ScramException;
import com.ongres.scram.common.util.TlsServerEndpoint;
import io.r2dbc.postgresql.client.ConnectionContext;
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
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SASLAuthenticationHandler implements AuthenticationHandler {

    private static final Logger LOG = Loggers.getLogger(SASLAuthenticationHandler.class);

    private final CharSequence password;

    private final String username;

    private final ConnectionContext context;

    private ScramClient scramClient;

    /**
     * Create a new handler.
     *
     * @param password the password to use for authentication
     * @param username the username to use for authentication
     * @param context  the connection context
     * @throws IllegalArgumentException if {@code password} or {@code user} is {@code null}
     */
    public SASLAuthenticationHandler(CharSequence password, String username, ConnectionContext context) {
        this.password = Assert.requireNonNull(password, "password must not be null");
        this.username = Assert.requireNonNull(username, "username must not be null");
        this.context = Assert.requireNonNull(context, "context must not be null");
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

        char[] password = new char[this.password.length()];
        for (int i = 0; i < password.length; i++) {
            password[i] = this.password.charAt(i);
        }

        ScramClient.FinalBuildStage builder = ScramClient.builder()
            .advertisedMechanisms(message.getAuthenticationMechanisms())
            .username(this.username) // ignored by the server, use startup message
            .password(password)
            .stringPreparation(StringPreparation.POSTGRESQL_PREPARATION);

        SSLSession sslSession = this.context.getSslSession();

        if (sslSession != null && sslSession.isValid()) {
            builder.channelBinding(TlsServerEndpoint.TLS_SERVER_END_POINT, extractSslEndpoint(sslSession));
        }

        this.scramClient = builder.build();

        return new SASLInitialResponse(ByteBufferUtils.encode(this.scramClient.clientFirstMessage().toString()), this.scramClient.getScramMechanism().getName());
    }

    private static byte[] extractSslEndpoint(SSLSession sslSession) {
        try {
            Certificate[] certificates = sslSession.getPeerCertificates();
            if (certificates != null && certificates.length > 0) {
                Certificate peerCert = certificates[0]; // First certificate is the peer's certificate
                if (peerCert instanceof X509Certificate) {
                    X509Certificate cert = (X509Certificate) peerCert;
                    return TlsServerEndpoint.getChannelBindingData(cert);

                }
            }
        } catch (CertificateException | SSLException e) {
            LOG.debug("Cannot extract X509Certificate from SSL session", e);
        }
        return new byte[0];
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
