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
import java.util.List;
import java.util.function.Predicate;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CodecFinderDefaultImplUnitTest {

    DefaultCodecs codecs;

    @Spy
    CodecFinderDefaultImpl codecFinder = new CodecFinderDefaultImpl();

    @Captor
    ArgumentCaptor<Predicate<Codec<?>>> predicateArgumentCaptor;

    @Mock
    Codec<String> stringCodec;

    @Mock
    Codec<Integer> integerCodec;

    @BeforeEach
    void setUp() {
        // We use the DefaultCodecs to populate the cache with some codecs
        codecs = new DefaultCodecs(TEST, false, codecFinder);
    }

    @Test
    void findCodec_notFound() {
        List<Codec<?>> codecList = Arrays.asList(stringCodec, integerCodec);
        codecFinder.updateCodecs(codecList);
        doReturn(false).when(stringCodec).canEncode(this);
        doReturn(false).when(integerCodec).canEncode(this);
        assertThat(codecFinder.findCodec(c -> c.canEncode(this))).isNull();
    }

    @Test
    void findCodec_found() {
        List<Codec<?>> codecList = Arrays.asList(stringCodec, integerCodec);
        codecFinder.updateCodecs(codecList);
        doReturn(false).when(stringCodec).canEncode(this);
        doReturn(true).when(integerCodec).canEncode(this);
        assertThat(codecFinder.findCodec(c -> c.canEncode(this))).isEqualTo(integerCodec);
    }

    @Test
    void findDecodeCodecShort() {
        Codec<Short> shortCodec = codecFinder.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, Short.class);
        assertThat(shortCodec).isNotNull();
        verify(codecFinder).findCodec(predicateArgumentCaptor.capture());
    }

    @Test
    void findDecodeCodecNotFound() {
        assertThat(codecFinder.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, this.getClass())).isNull();
    }

    @Test
    void findEncodeCodecDouble() {
        Codec<?> doubleCodec = codecFinder.findEncodeCodec(1.2);
        assertThat(doubleCodec).isInstanceOf(DoubleCodec.class);
        verify(codecFinder).findCodec(predicateArgumentCaptor.capture());
    }

    @Test
    void findEncodeCodecNotFound() {
        assertThat(codecFinder.findEncodeCodec(this)).isNull();
    }

    @Test
    void findEncodeNullCodecInteger() {
        Codec<?> intCodec = codecFinder.findEncodeNullCodec(Integer.class);
        assertThat(intCodec).isInstanceOf(IntegerCodec.class);
        verify(codecFinder).findCodec(predicateArgumentCaptor.capture());
    }

    @Test
    void findEncodeNullCodecNotFound() {
        assertThat(codecFinder.findEncodeNullCodec(DefaultCodecsUnitTests.class)).isNull();
    }

}