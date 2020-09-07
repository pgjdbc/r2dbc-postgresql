/*
 * Copyright 2020 the original author or authors.
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

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.message.backend.ErrorResponse;
import io.r2dbc.postgresql.message.backend.NoticeResponse;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.LogLevel;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.util.annotation.Nullable;

import java.net.Socket;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Value object capturing common connection settings.
 *
 * @since 0.9
 */
public final class ConnectionSettings {

    @Nullable
    private final Duration connectTimeout;

    private final ConnectionProvider connectionProvider;

    @Nullable
    private final LoopResources loopResources;

    private final SSLConfig sslConfig;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    private final LogLevel errorResponseLogLevel;

    private final LogLevel noticeLogLevel;

    ConnectionSettings(@Nullable Duration connectTimeout, ConnectionProvider connectionProvider, @Nullable LoopResources loopResources,
                       SSLConfig sslConfig, boolean tcpKeepAlive, boolean tcpNoDelay, LogLevel errorResponseLogLevel, LogLevel noticeLogLevel) {
        this.connectTimeout = connectTimeout;
        this.connectionProvider = connectionProvider;
        this.loopResources = loopResources;
        this.sslConfig = sslConfig;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.errorResponseLogLevel = errorResponseLogLevel;
        this.noticeLogLevel = noticeLogLevel;
    }

    /**
     * Return a new {@link Builder}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Return a {@link Builder} that is pre-initialized with the current values.
     *
     * @return a {@link Builder} that is pre-initialized with the current values
     */
    public Builder mutate() {
        return new Builder().connectionProvider(this.connectionProvider).loopResources(this.loopResources)
            .errorResponseLogLevel(this.errorResponseLogLevel).noticeLogLevel(this.noticeLogLevel).sslConfig(this.sslConfig)
            .connectTimeout(this.connectTimeout).tcpKeepAlive(this.tcpKeepAlive).tcpNoDelay(this.tcpNoDelay);
    }

    /**
     * Apply a {@link Consumer mutator} that creates a new {@link ConnectionSettings} object with the mutations applied.
     *
     * @return the mutated {@link ConnectionSettings}
     */
    public ConnectionSettings mutate(Consumer<Builder> mutator) {

        Builder builder = mutate();
        mutator.accept(builder);
        return builder.build();
    }

    boolean hasConnectionTimeout() {
        return this.connectTimeout != null;
    }

    int getConnectTimeoutMs() {

        if (this.connectTimeout == null) {
            throw new IllegalStateException("No connect timeout configured");
        }

        return Math.toIntExact(this.connectTimeout.toMillis());
    }

    ConnectionProvider getConnectionProvider() {
        return this.connectionProvider;
    }

    boolean hasLoopResources() {
        return this.loopResources != null;
    }

    LoopResources getRequiredLoopResources() {

        if (!hasLoopResources()) {
            throw new IllegalStateException("No LoopResources configured");
        }

        return this.loopResources;
    }

    SSLConfig getSslConfig() {
        return this.sslConfig;
    }

    boolean isTcpKeepAlive() {
        return this.tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return this.tcpNoDelay;
    }

    LogLevel getErrorResponseLogLevel() {
        return this.errorResponseLogLevel;
    }

    LogLevel getNoticeLogLevel() {
        return this.noticeLogLevel;
    }

    /**
     * A builder for {@link ConnectionSettings} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private Duration connectTimeout;

        private ConnectionProvider connectionProvider = ConnectionProvider.newConnection();

        private LoopResources loopResources = null;

        private LogLevel errorResponseLogLevel = LogLevel.WARN;

        private LogLevel noticeLogLevel = LogLevel.DEBUG;

        private SSLConfig sslConfig = new SSLConfig(SSLMode.DISABLE, null, null);

        private boolean tcpKeepAlive;

        private boolean tcpNoDelay;

        private Builder() {
        }

        /**
         * Returns a configured {@link ConnectionSettings}.
         *
         * @return a configured {@link ConnectionSettings}
         */
        public ConnectionSettings build() {
            return new ConnectionSettings(this.connectTimeout, this.connectionProvider, this.loopResources, this.sslConfig,
                this.tcpKeepAlive, this.tcpNoDelay, this.errorResponseLogLevel, this.noticeLogLevel);
        }

        /**
         * Configure the connection timeout. Default unconfigured.
         *
         * @param connectTimeout the connection timeout
         * @return this {@link Builder}
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Configure the {@link ConnectionProvider}.
         *
         * @param connectionProvider the connection provider
         * @return this {@link Builder}
         */
        public Builder connectionProvider(ConnectionProvider connectionProvider) {
            this.connectionProvider = Assert.requireNonNull(connectionProvider, "connectionProvider must not be null");
            return this;
        }

        /**
         * Configure the {@link LoopResources}.
         * The {@link LoopResources}'s lifecycle should be managed externally.
         *
         * @param loopResources the {@link LoopResources}.
         * @return this {@link Builder}
         * @since 0.8.5
         */
        public Builder loopResources(@Nullable LoopResources loopResources) {
            this.loopResources = loopResources;
            return this;
        }

        /**
         * Configure the {@link LogLevel} for {@link ErrorResponse error responses} that are part of a statement execution.
         *
         * @param errorResponseLogLevel the log level to use.
         * @return this {@link Builder}
         */
        public Builder errorResponseLogLevel(LogLevel errorResponseLogLevel) {
            this.errorResponseLogLevel = Assert.requireNonNull(errorResponseLogLevel, "errorResponseLogLevel must not be null");
            return this;
        }

        /**
         * Configure the {@link LogLevel} for {@link NoticeResponse notice responses}.
         *
         * @param noticeLogLevel the log level to use.
         * @return this {@link Builder}
         * @since 0.9
         */
        public Builder noticeLogLevel(LogLevel noticeLogLevel) {
            this.noticeLogLevel = Assert.requireNonNull(noticeLogLevel, "noticeLogLevel must not be null");
            return this;
        }

        /**
         * Configure the {@link SSLConfig SSL settings.}
         *
         * @param sslConfig the SSL configuration
         * @return this {@link Builder}
         */
        public Builder sslConfig(SSLConfig sslConfig) {
            this.sslConfig = Assert.requireNonNull(sslConfig, "sslConfig must not be null");
            return this;
        }

        /**
         * Configure TCP KeepAlive.
         *
         * @param enabled whether to enable TCP KeepAlive
         * @return this {@link PostgresqlConnectionConfiguration.Builder}
         * @see Socket#setKeepAlive(boolean)
         */
        public Builder tcpKeepAlive(boolean enabled) {
            this.tcpKeepAlive = enabled;
            return this;
        }

        /**
         * Configure TCP NoDelay.
         *
         * @param enabled whether to enable TCP NoDelay
         * @return this {@link PostgresqlConnectionConfiguration.Builder}
         * @see Socket#setTcpNoDelay(boolean)
         */
        public Builder tcpNoDelay(boolean enabled) {
            this.tcpNoDelay = enabled;
            return this;
        }

    }

}
