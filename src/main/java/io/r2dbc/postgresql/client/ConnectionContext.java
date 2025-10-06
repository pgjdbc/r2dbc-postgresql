/*
 * Copyright 2019 the original author or authors.
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

package io.r2dbc.postgresql.client;

import org.jspecify.annotations.Nullable;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.net.ssl.SSLSession;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Value object capturing diagnostic connection context. Allows for log-message post-processing with {@link #getMessage(String) if the logger category for
 * {@code ConnectionContext} is enabled for DEBUG/TRACE logs.
 * <p>
 * Captures also the Postgres process Id.
 *
 * @since 0.8.6
 */
public final class ConnectionContext {

    private final static Logger LOGGER = Loggers.getLogger(ConnectionContext.class.getName() + ".context");

    private final static boolean CONTEXT_ENABLED = LOGGER.isDebugEnabled();

    private final static boolean CHANNEL_ID_ENABLED = LOGGER.isTraceEnabled();

    private static final AtomicLong CONN_ID = new AtomicLong();

    private final @Nullable Integer processId;

    private final @Nullable String channelId;

    private final String connectionCounter;

    private final String connectionIdPrefix;

    private final Supplier<SSLSession> sslSession;

    /**
     * Create a new {@link ConnectionContext} with a unique connection Id.
     */
    public ConnectionContext() {
        this.processId = null;
        this.connectionCounter = incrementConnectionCounter();
        this.connectionIdPrefix = getConnectionIdPrefix();
        this.channelId = null;
        this.sslSession = () -> null;
    }

    private ConnectionContext(@Nullable Integer processId, @Nullable String channelId, String connectionCounter, Supplier<SSLSession> sslSession) {
        this.processId = processId;
        this.channelId = channelId;
        this.connectionCounter = connectionCounter;
        this.connectionIdPrefix = getConnectionIdPrefix();
        this.sslSession = sslSession;
    }

    private String incrementConnectionCounter() {
        return Long.toHexString(CONN_ID.incrementAndGet());
    }

    private String getConnectionIdPrefix() {

        String prefix = String.format("[cid: 0x%s]", this.connectionCounter);

        if (this.processId != null) {
            prefix += String.format("[pid: %d]", this.processId);
        }

        if (CHANNEL_ID_ENABLED && this.channelId != null) {
            prefix += String.format("%s", this.channelId);
        }

        return prefix + " ";
    }

    /**
     * Process the {@code original} message to inject potentially debug information such as the channel Id or the process Id.
     *
     * @param original the original message
     * @return the post-processed log message
     */
    public String getMessage(String original) {

        if (CONTEXT_ENABLED) {
            return this.connectionIdPrefix + original;
        }

        return original;
    }

    public @Nullable SSLSession getSslSession() {
        return this.sslSession.get();
    }

    /**
     * Create a new {@link ConnectionContext} by associating the {@code channelId}.
     *
     * @param channelId the channel identifier.
     * @return a new {@link ConnectionContext} with all previously set values and the associated {@code channelId}.
     */
    public ConnectionContext withChannelId(String channelId) {
        return new ConnectionContext(this.processId, channelId, this.connectionCounter, this.sslSession);
    }

    /**
     * Create a new {@link ConnectionContext} by associating the {@code sslSession}.
     *
     * @param sslSession the SSL session supplier.
     * @return a new {@link ConnectionContext} with all previously set values and the associated {@code sslSession}.
     */
    public ConnectionContext withSslSession(Supplier<SSLSession> sslSession) {
        return new ConnectionContext(this.processId, this.channelId, this.connectionCounter, sslSession);
    }

    /**
     * Create a new {@link ConnectionContext} by associating the {@code processId}.
     *
     * @param processId the Postgres processId.
     * @return a new {@link ConnectionContext} with all previously set values and the associated {@code processId}.
     */
    public ConnectionContext withProcessId(int processId) {
        return new ConnectionContext(processId, this.channelId, this.connectionCounter, this.sslSession);
    }

}
