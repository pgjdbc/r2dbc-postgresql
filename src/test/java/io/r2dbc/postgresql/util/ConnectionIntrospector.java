/*
 * Copyright 2020-2020 the original author or authors.
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

import io.netty.channel.Channel;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.client.ReactorNettyClient;
import org.springframework.beans.DirectFieldAccessor;
import reactor.netty.Connection;

/**
 * Utility to introspect a {@link PostgresqlConnection}.
 */
public final class ConnectionIntrospector {

    private final PostgresqlConnection connection;

    private ConnectionIntrospector(PostgresqlConnection connection) {
        this.connection = connection;
    }

    /**
     * Create a new instance.
     *
     * @param connection
     * @return
     */
    public static ConnectionIntrospector of(PostgresqlConnection connection) {
        return new ConnectionIntrospector(connection);
    }

    /**
     * Return the transport {@link Channel}.
     *
     * @return the transport {@link Channel}.
     */
    public Channel getChannel() {

        DirectFieldAccessor accessor = new DirectFieldAccessor(getClient());
        Connection connection = (Connection) accessor.getPropertyValue("connection");

        return connection.channel();
    }

    /**
     * Return the underlying {@link ReactorNettyClient}.
     *
     * @return the underlying {@link ReactorNettyClient}.
     */
    public ReactorNettyClient getClient() {

        DirectFieldAccessor accessor = new DirectFieldAccessor(this.connection);
        Object value = accessor.getPropertyValue("client");

        Assert.requireType(value, ReactorNettyClient.class, "Client must be of type ReactorNettyClient. Was: " + value.getClass().getName());

        return (ReactorNettyClient) value;
    }
}
