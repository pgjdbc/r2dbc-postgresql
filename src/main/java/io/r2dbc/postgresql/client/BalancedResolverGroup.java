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

import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.RoundRobinInetAddressResolver;
import io.netty.util.concurrent.EventExecutor;

import java.net.InetSocketAddress;

/**
 * When the {@link InetSocketAddress} resolves to multiple IP addresses, pick one randomly.
 *
 * @since 0.8.6
 */
final class BalancedResolverGroup extends AddressResolverGroup<InetSocketAddress> {

    BalancedResolverGroup() {
    }

    public static final BalancedResolverGroup INSTANCE;

    static {

        INSTANCE = new BalancedResolverGroup();
        Runtime.getRuntime().addShutdownHook(new Thread(INSTANCE::close, "R2DBC-Postgresql-BalancedResolverGroup-ShutdownHook"));
    }

    @Override
    protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) throws Exception {
        return new RoundRobinInetAddressResolver(executor, new DefaultNameResolver(executor)).asAddressResolver();
    }

}
