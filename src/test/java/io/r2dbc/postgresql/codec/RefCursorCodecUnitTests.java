/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.postgresql.api.RefCursor;
import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.REF_CURSOR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link RefCursorCodec}.
 */
final class RefCursorCodecUnitTests {

    private static final int dataType = REF_CURSOR.getObjectId();

    @Test
    void decode() {
        RefCursor refCursor = new RefCursorCodec().decode(encode(TEST, "test"), dataType, FORMAT_TEXT, RefCursor.class);
        assertThat(refCursor.getCursorName())
            .isEqualTo("test");
    }

    @Test
    void detachedRefCursorDoesNotSupportFetching() {
        RefCursor refCursor = new RefCursorCodec().decode(encode(TEST, "test"), dataType, FORMAT_TEXT, RefCursor.class);
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(refCursor::fetch);
    }

    @Test
    void detachedRefCursorDoesNotSupportClosing() {
        RefCursor refCursor = new RefCursorCodec().decode(encode(TEST, "test"), dataType, FORMAT_TEXT, RefCursor.class);
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(refCursor::close);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new RefCursorCodec().decode(null, dataType, FORMAT_TEXT, RefCursor.class)).isNull();
    }

    @Test
    void doCanDecode() {
        RefCursorCodec codec = RefCursorCodec.INSTANCE;

        assertThat(codec.doCanDecode(REF_CURSOR, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
    }

}
