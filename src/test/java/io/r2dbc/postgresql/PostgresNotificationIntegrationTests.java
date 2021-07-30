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

package io.r2dbc.postgresql;

import io.netty.channel.Channel;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.util.ConnectionIntrospector;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link Notification} through {@link PostgresqlConnection#getNotifications()}.
 */
final class PostgresNotificationIntegrationTests extends AbstractIntegrationTests {

    @Test
    void shouldReceivePubSubNotifications() throws Exception {

        BlockingQueue<Notification> notifications = new LinkedBlockingQueue<>();

        CountDownLatch await = new CountDownLatch(1);
        Disposable listener = this.connectionFactory.create().flatMapMany(it -> {
            return it.createStatement("LISTEN mymessage").execute().flatMap(PostgresqlResult::getRowsUpdated).doOnComplete(await::countDown)
                .thenMany(it.getNotifications()).doOnCancel(() -> it.close().subscribe());
        }).doOnNext(notifications::add).subscribe();

        await.await(10, TimeUnit.SECONDS);

        this.connectionFactory.create().flatMapMany(it -> it.createStatement("NOTIFY mymessage, 'Mötorhead'").execute().flatMap(PostgresqlResult::getRowsUpdated).thenMany(it.close()))
            .as(StepVerifier::create).verifyComplete();

        Notification notification = notifications.poll(10, TimeUnit.SECONDS);

        assertThat(notification).isNotNull();
        assertThat(notification.getName()).isEqualTo("mymessage");
        assertThat(notification.getProcessId()).isNotZero();
        assertThat(notification.getParameter()).isEqualTo("Mötorhead");

        listener.dispose();
    }

    @Test
    void listenShouldCompleteOnConnectionClose() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.getNotifications().as(StepVerifier::create).expectSubscription()
            .then(() -> connection.close().subscribe())
            .verifyComplete();
    }

    @Test
    void listenShouldFailOnConnectionDisconnected() {

        PostgresqlConnection connection = this.connectionFactory.create().block();

        connection.getNotifications().as(StepVerifier::create).expectSubscription()
            .then(() -> {

                Channel channel = ConnectionIntrospector.of(connection).getChannel();
                channel.close();
            })
            .verifyError(R2dbcNonTransientResourceException.class);
    }

}
