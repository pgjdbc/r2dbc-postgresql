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

package io.r2dbc.postgresql.replication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ReplicationSlotRequest}.
 */
final class ReplicationSlotRequestUnitTests {

    @Test
    void rejectsInvalidSlot() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.logical().slotName(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.logical().slotName(""));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.physical().slotName(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.physical().slotName(""));
    }

    @Test
    void rejectsInvalidOutputPlugin() {
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.logical().slotName("foo").outputPlugin(null));
        assertThatIllegalArgumentException().isThrownBy(() -> ReplicationSlotRequest.logical().slotName("foo").outputPlugin(""));
    }

    @Test
    void buildsLogicalRequest() {
        ReplicationSlotRequest request = ReplicationSlotRequest.logical().slotName("slot").outputPlugin("output").temporary().build();
        assertThat(request.getReplicationType()).isEqualTo(ReplicationType.LOGICAL);
        assertThat(request.getSlotName()).isEqualTo("slot");
        assertThat(request.isTemporary()).isTrue();
        assertThat(request.asSQL()).isEqualTo("CREATE_REPLICATION_SLOT slot TEMPORARY LOGICAL output");
    }

    @Test
    void buildsPhysicalRequest() {
        ReplicationSlotRequest request = ReplicationSlotRequest.physical().slotName("slot").temporary().build();
        assertThat(request.getReplicationType()).isEqualTo(ReplicationType.PHYSICAL);
        assertThat(request.getSlotName()).isEqualTo("slot");
        assertThat(request.isTemporary()).isTrue();
        assertThat(request.asSQL()).isEqualTo("CREATE_REPLICATION_SLOT slot TEMPORARY PHYSICAL");
    }

}
