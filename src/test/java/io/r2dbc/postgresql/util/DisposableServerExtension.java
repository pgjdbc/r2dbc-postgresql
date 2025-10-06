/*
 * Copyright 2017 the original author or authors.
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

package io.r2dbc.postgresql.util;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import reactor.netty.DisposableChannel;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JUnit Extension to create a disposable server that does not reply, only accepting connections.
 */
final class DisposableServerExtension implements ParameterResolver, AfterAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(DisposableServerExtension.class);

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.isAnnotated(Disposable.class) && parameterContext.getParameter().getType().isAssignableFrom(InetSocketAddress.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {

        ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
        Map<Parameter, DisposableServer> servers = store.computeIfAbsent("servers", key -> new ConcurrentHashMap<>(), Map.class);


        DisposableServer server = servers.computeIfAbsent(parameterContext.getParameter(), key -> newServer());
        return InetSocketAddress.createUnresolved(server.host(), server.port());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterAll(ExtensionContext extensionContext) {

        ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
        Map<Parameter, DisposableServer> servers = store.get("servers", Map.class);

        if (servers != null) {
            for (DisposableServer server : servers.values()) {
                server.disposeNow();
            }
            servers.clear();
        }

    }

    static DisposableServer newServer() {
        return TcpServer.create()
            .doOnConnection(DisposableChannel::dispose)
            .bindNow();
    }


}
