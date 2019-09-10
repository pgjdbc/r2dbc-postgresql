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

import io.r2dbc.postgresql.util.PostgresqlServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

final class PostgresNotificationIntegrationTest {

    @RegisterExtension
    static final PostgresqlServerExtension SERVER = new PostgresqlServerExtension();

    private final PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder()
        .database(SERVER.getDatabase())
        .host(SERVER.getHost())
        .port(SERVER.getPort())
        .password(SERVER.getPassword())
        .username(SERVER.getUsername())
        .forceBinary(true)
        .build();

    private final PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(this.configuration);

    @Test
    void shouldReceivePubSubNotifications() throws Exception {

        BlockingQueue<Notification> notifications = new LinkedBlockingQueue<>();

        CountDownLatch await = new CountDownLatch(1);
        Disposable listener = connectionFactory.create().flatMapMany(it -> {
            return it.createStatement("LISTEN mymessage").execute().doOnComplete(await::countDown)
                .thenMany(it.getNotifications()).doOnCancel(() -> it.close().subscribe());
        }).doOnNext(notifications::add).subscribe();

        await.await(10, TimeUnit.SECONDS);

        connectionFactory.create().flatMapMany(it -> it.createStatement("NOTIFY mymessage, 'Mötorhead'").execute().thenMany(it.close()))
            .as(StepVerifier::create).verifyComplete();

        Notification notification = notifications.poll(10, TimeUnit.SECONDS);

        assertThat(notification).isNotNull();
        assertThat(notification.getName()).isEqualTo("mymessage");
        assertThat(notification.getProcessId()).isNotZero();
        assertThat(notification.getParameter()).isEqualTo("Mötorhead");

        listener.dispose();
    }
}
