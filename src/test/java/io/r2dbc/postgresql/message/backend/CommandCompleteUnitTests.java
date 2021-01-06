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

package io.r2dbc.postgresql.message.backend;

import org.junit.jupiter.api.Test;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.r2dbc.postgresql.message.backend.BackendMessageAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link CommandComplete}.
 */
final class CommandCompleteUnitTests {

    @Test
    void constructorNoCommand() {
        assertThatIllegalArgumentException().isThrownBy(() -> new CommandComplete(null, 100, 200))
            .withMessage("command must not be null");
    }

    @Test
    void decodeCopy() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("COPY 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("COPY", null, 100));
    }

    @Test
    void decodeDelete() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("DELETE 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("DELETE", null, 100));
    }

    @Test
    void decodeFetch() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("FETCH 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("FETCH", null, 100));
    }

    @Test
    void decodeInsert() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("INSERT 100 200", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("INSERT", 100, 200));
    }

    @Test
    void decodeMove() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("MOVE 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("MOVE", null, 100));
    }

    @Test
    void decodeOther() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("CREATE TABLE", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("CREATE TABLE", null, null));
    }

    @Test
    void decodeSelect() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("SELECT 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("SELECT", null, 100));
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("SELECT", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("SELECT", null, null));
    }

    @Test
    void decodeUpdate() {
        assertThat(CommandComplete.class)
            .decoded(buffer -> {
                buffer.writeCharSequence("UPDATE 100", UTF_8);
                buffer.writeByte(0);

                return buffer;
            })
            .isEqualTo(new CommandComplete("UPDATE", null, 100));
    }

}
