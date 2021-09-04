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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@ExtendWith(MockitoExtension.class)
class CodecFinderCacheImplUnitTest {

    DefaultCodecs codecs;

    CodecFinderCacheImpl codecFinder;

    @Spy
    CodecFinderDefaultImpl fallBackFinder = new CodecFinderDefaultImpl();

    @Captor
    ArgumentCaptor<Map<Integer, Codec<?>>> cacheArgumentCaptor;

    @Mock
    Codec<String> stringCodec;

    @Mock
    Codec<Integer> integerCodec;

    @BeforeEach
    void setUp() {
        codecFinder = new CodecFinderCacheImpl(fallBackFinder);
        // We use the DefaultCodecs to populate the cache with some codecs
        codecs = new DefaultCodecs(TEST, false, codecFinder);
    }

    @Test
    void fallbackFinder_same_class() {
        assertThatThrownBy(() -> new CodecFinderCacheImpl(codecFinder)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findCodec() {
        List<Codec<?>> codecList = Arrays.asList(stringCodec, integerCodec);
        doReturn(String.class).when(stringCodec).type();
        doReturn(Collections.singleton((PostgresTypeIdentifier) VARCHAR)).when(stringCodec).getDataTypes();
        doReturn(EnumSet.of(FORMAT_TEXT)).when(stringCodec).getFormats();
        doReturn(Integer.class).when(integerCodec).type();
        doReturn(Collections.singleton((PostgresTypeIdentifier) INT4)).when(integerCodec).getDataTypes();
        doReturn(EnumSet.of(FORMAT_TEXT)).when(integerCodec).getFormats();
        codecFinder.updateCodecs(codecList);
        assertThat(codecFinder.findDecodeCodec(INT4.getObjectId(), FORMAT_TEXT, Integer.class)).isEqualTo(integerCodec);
        assertThat(codecFinder.findDecodeCodec(VARCHAR.getObjectId(), FORMAT_TEXT, String.class)).isEqualTo(stringCodec);
        assertThat(codecFinder.findDecodeCodec(FLOAT8.getObjectId(), FORMAT_TEXT, Double.class)).isNull();
    }

    @Test
    void findDecodeCodecShort() {
        CodecFinderCacheImpl spyCodecs = spy(codecFinder);
        Codec<Short> shortCodec = spyCodecs.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, Short.class);
        assertThat(shortCodec).isNotNull();
    }

    @Test
    void findDecodeCodecNotFound() {
        assertThat(codecFinder.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, DefaultCodecsUnitTests.class)).isNull();
    }

    @Test
    void findEncodeCodecDouble() {
        CodecFinderCacheImpl spyCodecs = spy(codecFinder);
        Codec<?> doubleCodec = spyCodecs.findEncodeCodec(1.2);
        assertThat(doubleCodec).isInstanceOf(DoubleCodec.class);
    }

    @Test
    void findEncodeCodecNotFound() {
        assertThat(codecFinder.findEncodeCodec(this)).isNull();
    }

    @Test
    void findEncodeNullCodecInteger() {
        CodecFinderCacheImpl spyCodecs = spy(codecFinder);
        Codec<?> intCodec = spyCodecs.findEncodeNullCodec(Integer.class);
        assertThat(intCodec).isInstanceOf(IntegerCodec.class);
    }

    @Test
    void findEncodeNullCodecNotFound() {
        assertThat(codecFinder.findEncodeNullCodec(DefaultCodecsUnitTests.class)).isNull();
    }

}