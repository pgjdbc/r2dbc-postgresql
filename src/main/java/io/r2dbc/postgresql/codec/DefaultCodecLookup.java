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

import io.r2dbc.postgresql.message.Format;
import org.jspecify.annotations.Nullable;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * Default implementation of the {@link CodecLookup}. This will systematically search for {@link Codec} for the type, format and database type
 * by calling the methods {@link Codec#canDecode}, {@link Codec#canEncode} and {@link Codec#canEncodeNull} on each registered codecs.
 *
 * @since 0.9
 */
class DefaultCodecLookup implements CodecLookup {

    private final Iterable<Codec<?>> codecs;

    DefaultCodecLookup(Iterable<Codec<?>> codecRegistry) {
        this.codecs = codecRegistry;
    }

    @SuppressWarnings("unchecked")
    synchronized <T> @Nullable Codec<T> findCodec(Predicate<Codec<?>> predicate) {

        for (Codec<?> codec : this.codecs) {
            if (predicate.test(codec)) {
                return (Codec<T>) codec;
            }
        }
        return null;
    }

    @Override
    public <T> Codec<T> findDecodeCodec(int dataType, Format format, Class<? extends T> type) {
        return findCodec(codec -> codec.canDecode(dataType, format, type));
    }

    @Override
    public <T> Codec<T> findEncodeCodec(T value) {
        return findCodec(codec -> codec.canEncode(value));
    }

    @Override
    public <T> Codec<T> findEncodeNullCodec(Class<T> type) {
        return findCodec(codec -> codec.canEncodeNull(type));
    }

    @Override
    public Iterator<Codec<?>> iterator() {
        return this.codecs.iterator();
    }

}
