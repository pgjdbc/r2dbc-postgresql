/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for {@link CachedCodecLookup}.
 */
@ExtendWith(MockitoExtension.class)
class CachedCodecLookupUnitTest {

    private static final int SOME_OID = PostgresqlObjectId.OID_CACHE_SIZE * 2;

    DefaultCodecs codecs;

    CachedCodecLookup codecFinder;

    @Mock
    AbstractCodec<String> stringCodec;

    @Mock
    AbstractCodec<Integer> integerCodec;

    @BeforeEach
    void setUp() {
        // We use the DefaultCodecs to populate the cache with some codecs
        this.codecs = new DefaultCodecs(TEST, false);
        this.codecFinder = new CachedCodecLookup(this.codecs);
        this.codecFinder.afterCodecAdded();
    }

    @Test
    void fallbackFinder_same_class() {
        assertThatThrownBy(() -> new CachedCodecLookup(this.codecFinder)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findCodec() {
        List<Codec<?>> codecList = Arrays.asList(this.stringCodec, this.integerCodec);
        doReturn(String.class).when(this.stringCodec).type();
        doReturn(Collections.singleton((PostgresTypeIdentifier) VARCHAR)).when(this.stringCodec).getDataTypes();
        doReturn(EnumSet.of(FORMAT_TEXT)).when(this.stringCodec).getFormats();
        doReturn(Integer.class).when(this.integerCodec).type();
        doReturn(Collections.singleton((PostgresTypeIdentifier) INT4)).when(this.integerCodec).getDataTypes();
        doReturn(EnumSet.of(FORMAT_TEXT)).when(this.integerCodec).getFormats();
        codecList.forEach(this.codecs::addFirst);
        this.codecFinder.afterCodecAdded();

        assertThat(this.codecFinder.findDecodeCodec(INT4.getObjectId(), FORMAT_TEXT, Integer.class)).isEqualTo(this.integerCodec);
        assertThat(this.codecFinder.findDecodeCodec(VARCHAR.getObjectId(), FORMAT_TEXT, String.class)).isEqualTo(this.stringCodec);
        assertThat(this.codecFinder.findDecodeCodec(UNKNOWN.getObjectId(), FORMAT_TEXT, Double.class)).isNull();
    }

    @Test
    void findDecodeCodecShort() {
        CachedCodecLookup spyCodecs = spy(this.codecFinder);
        Codec<Short> shortCodec = spyCodecs.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, Short.class);
        assertThat(shortCodec).isNotNull();
    }

    @Test
    void findDecodeCodecNotFound() {
        assertThat(this.codecFinder.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, DefaultCodecsUnitTests.class)).isNull();
    }

    @Test
    void findEncodeCodecDouble() {
        CachedCodecLookup spyCodecs = spy(this.codecFinder);
        Codec<?> doubleCodec = spyCodecs.findEncodeCodec(1.2);
        assertThat(doubleCodec).isInstanceOf(DoubleCodec.class);
    }

    @Test
    void findEncodeCodecNotFound() {
        assertThat(this.codecFinder.findEncodeCodec(this)).isNull();
    }

    @Test
    void findEncodeNullCodecInteger() {
        CachedCodecLookup spyCodecs = spy(this.codecFinder);
        Codec<?> intCodec = spyCodecs.findEncodeNullCodec(Integer.class);
        assertThat(intCodec).isInstanceOf(IntegerCodec.class);
    }

    @Test
    void findEncodeNullCodecNotFound() {
        assertThat(this.codecFinder.findEncodeNullCodec(DefaultCodecsUnitTests.class)).isNull();
    }

    @Test
    void codecWithoutMetadataKeepWorking() {

        this.codecs.addLast(EnumCodec.INSTANCE);
        this.codecFinder.afterCodecAdded();

        assertThat(this.codecFinder.findDecodeCodec(SOME_OID, FORMAT_TEXT, Health.class)).isEqualTo(EnumCodec.INSTANCE);
        assertThat(this.codecFinder.findDecodeCodec(SOME_OID, FORMAT_TEXT, Object.class)).isEqualTo(EnumCodec.INSTANCE);
        assertThat(this.codecFinder.findEncodeCodec(Health.OK)).isEqualTo(EnumCodec.INSTANCE);
        assertThat(this.codecFinder.findEncodeNullCodec(Health.class)).isEqualTo(EnumCodec.INSTANCE);
    }

    enum Health {OK, NOT_SO_MUCH}

    enum EnumCodec implements Codec<Health> {
        INSTANCE;

        @Override
        public boolean canDecode(int dataType, Format format, Class<?> type) {
            return dataType == SOME_OID;
        }

        @Override
        public boolean canEncode(Object value) {
            return value instanceof Health;
        }

        @Override
        public boolean canEncodeNull(Class<?> type) {
            return Health.class.isAssignableFrom(type);
        }

        @Override
        public Health decode(ByteBuf buffer, int dataType, Format format, Class<? extends Health> type) {
            return Health.OK;
        }

        @Override
        public EncodedParameter encode(Object value) {
            return new EncodedParameter(FORMAT_TEXT, SOME_OID, Mono.just(Unpooled.wrappedBuffer("OK".getBytes())));
        }

        @Override
        public EncodedParameter encode(Object value, int dataType) {
            return new EncodedParameter(FORMAT_TEXT, dataType, Mono.just(Unpooled.wrappedBuffer("OK".getBytes())));
        }

        @Override
        public EncodedParameter encodeNull() {
            return new EncodedParameter(FORMAT_TEXT, SOME_OID, Mono.just(Unpooled.wrappedBuffer("null".getBytes())));
        }
    }

}
