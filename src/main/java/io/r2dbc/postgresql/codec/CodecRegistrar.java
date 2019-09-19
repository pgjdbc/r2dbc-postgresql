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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.PostgresqlConnection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.ServiceLoader;

/**
 * Registrar interface that is discovered and instantiated using Java Service Loader to register extensions as {@link Codec}s.
 * <p>Implementations may use {@link PostgresqlConnection} to query Postgres information schema to discover type details such as extension type OIDs.
 *
 * @see ServiceLoader
 */
public interface CodecRegistrar {

    /**
     * Register {@link Codec} into {@link CodecRegistry}.
     * Codecs should be registered using a deferred approach upon {@link Publisher#subscribe(Subscriber) subscription}.
     *
     * @param connection the connection to query the connected Postgres runtime instance
     * @param allocator  the allocator for buffer allocation
     * @param registry   target codec registry that accepts codec registrations
     * @return a {@link Publisher} that activates codec registration upon subscription
     */
    Publisher<Void> register(PostgresqlConnection connection, ByteBufAllocator allocator, CodecRegistry registry);

}
