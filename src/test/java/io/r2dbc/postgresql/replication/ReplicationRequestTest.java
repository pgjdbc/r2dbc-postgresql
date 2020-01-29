/*
 * Copyright 2019-2020 the original author or authors.
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

package io.r2dbc.postgresql.replication;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ReplicationRequestTest {

    @Test
    void rejectsInvalidSlot() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.logical().slotName(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.logical().slotName(""));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.physical().slotName(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.physical().slotName(""));
    }

    @Test
    void rejectsInvalidOutputLsn() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.logical().slotName("foo").startPosition(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationRequest.physical().slotName("foo").startPosition(null));
    }

    @Test
    void buildsLogicalRequest() {
        ReplicationRequest request = ReplicationRequest.logical().slotName("slot")
            .startPosition(LogSequenceNumber.valueOf(0))
            .statusInterval(Duration.ofSeconds(11))
            .slotOption("skip-empty-xacts", true)
            .slotOption("include-xids", false).build();
        assertThat(request.getReplicationType()).isEqualTo(ReplicationType.LOGICAL);
        assertThat(request.getStatusInterval()).isEqualTo(Duration.ofSeconds(11));
        assertThat(request.asSQL()).isEqualTo("START_REPLICATION SLOT slot LOGICAL 0/0 (\"skip-empty-xacts\" 'true', \"include-xids\" 'false')");
    }

    @Test
    void buildsPhysicalRequest() {
        ReplicationRequest request = ReplicationRequest.physical().slotName("slot")
            .startPosition(LogSequenceNumber.valueOf(0))
            .statusInterval(Duration.ofSeconds(11)).build();
        assertThat(request.getReplicationType()).isEqualTo(ReplicationType.PHYSICAL);
        assertThat(request.getStatusInterval()).isEqualTo(Duration.ofSeconds(11));
        assertThat(request.asSQL()).isEqualTo("START_REPLICATION SLOT slot PHYSICAL 0/0");
    }

}
