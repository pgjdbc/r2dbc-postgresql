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

import reactor.netty.resources.LoopResources;
import reactor.util.annotation.Nullable;

import java.time.Duration;

/**
 * Value object capturing common connection settings.
 *
 * @since 0.8.4
 */
public final class ConnectionSettings {

    private final @Nullable
    Duration connectTimeout;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    @Nullable
    private final LoopResources tcpLoopResources;

    public ConnectionSettings(@Nullable Duration connectTimeout, boolean tcpKeepAlive, boolean tcpNoDelay, @Nullable LoopResources tcpLoopResources) {
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
        this.connectTimeout = connectTimeout;
        this.tcpLoopResources = tcpLoopResources;
    }

    @Nullable
    Duration getConnectTimeout() {
        return this.connectTimeout;
    }

    boolean isTcpKeepAlive() {
        return this.tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return this.tcpNoDelay;
    }

    public boolean hasTcpLoopResources() {
        return this.tcpLoopResources != null;
    }
    
    LoopResources getTcpLoopResources() {
        return this.tcpLoopResources;
    }

}
