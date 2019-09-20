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

package io.r2dbc.postgresql.extension;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.PostgresqlConnection;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Registrar interface that is used to register {@link Codec}s as extension to built-in codecs.
 * <p>Implementations may use {@link PostgresqlConnection} to query Postgres information schema to discover type details such as extension type OIDs.
 *
 * <h3>Constructor Requirements</h3>
 * <p>Extension implementations must have a <em>default constructor</em> if registered via the {@code ServiceLoader}.  When registered through
 * {@link io.r2dbc.postgresql.PostgresqlConnectionConfiguration} the default constructor is not required to be {@code public}.
 * When registered via the {@code ServiceLoader} the default constructor must be {@code public}.
 *
 * @see Extension
 */
@FunctionalInterface
public interface CodecRegistrar extends Extension {

    /**
     * Register {@link Codec} into {@link CodecRegistry}.
     * Codecs should be registered using a deferred approach upon {@link Publisher#subscribe(Subscriber) subscription}.
     *
     * @param connection the connection to query the connected Postgres runtime instance
     * @param allocator  the allocator for buffer allocation
     * @param registry   target codec registry that accepts codec registrations
     * @return a {@link Publisher} that activates codec registration upon subscription
     */
    Publisher<Void> register(Connection connection, ByteBufAllocator allocator, CodecRegistry registry);

}
