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

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache implementation of the {@link CodecLookup}. This will keep the relevant {@link Codec} for the type, format and database type cached for faster access.
 * In case the {@link Codec} can't be found in the cache, a fallback search using {@link DefaultCodecLookup} will be done.
 * <p>
 * Decode codecs are cached by their exact {@code (type, format, dataType)} key, so a cache hit is trusted without re-running
 * {@link Codec#canDecode(int, Format, Class)}. Lookups take no lock and do not allocate.
 *
 * @since 0.9
 */
class CachedCodecLookup implements CodecLookup {

    private static final Logger LOG = Loggers.getLogger(CachedCodecLookup.class);

    private final Map<Class<?>, DecodeCache> decodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Class<?>, Codec<?>> encodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Class<?>, Codec<?>> encodeNullCodecsCache = new ConcurrentHashMap<>();

    private final CodecLookup delegate;

    public CachedCodecLookup(Iterable<Codec<?>> codecRegistry) {
        this.delegate = new DefaultCodecLookup(codecRegistry);
    }

    public CachedCodecLookup(CodecLookup delegate) {
        Assert.requireNonType(delegate, CachedCodecLookup.class, "delegate must not be of type CodecFinderCacheImpl");
        this.delegate = delegate;
    }

    @Override
    public Iterator<Codec<?>> iterator() {
        return this.delegate.iterator();
    }

    @Override
    public void afterCodecAdded() {

        this.decodeCodecsCache.clear();
        this.encodeCodecsCache.clear();
        this.encodeNullCodecsCache.clear();

        for (Codec<?> c : this.delegate) {
            Optional<Class<?>> arrayClass = Optional.empty();
            if (c instanceof ArrayCodec) {
                ArrayCodec<?> ac = (ArrayCodec<?>) c;
                arrayClass = Optional.of(Array.newInstance(ac.getComponentType(), 0).getClass());
            }

            if (c instanceof CodecMetadata) {
                CodecMetadata metadata = (CodecMetadata) c;
                cacheEncode(c, metadata.type());
                arrayClass.ifPresent(ac -> cacheEncode(c, ac));
                for (PostgresTypeIdentifier identifier : metadata.getDataTypes()) {
                    for (Format format : metadata.getFormats()) {
                        cacheDecode(c, metadata.type(), identifier.getObjectId(), format);
                        arrayClass.ifPresent(ac -> cacheDecode(c, ac, identifier.getObjectId(), format));
                    }
                }
            }
        }
        // Handle decode to Object.class support
        for (PostgresqlObjectId identifier : PostgresqlObjectId.values()) {
            for (Format format : Format.all()) {
                Codec<?> c = this.delegate.findDecodeCodec(identifier.getObjectId(), format, Object.class);
                if (c != null) {
                    cacheDecode(c, Object.class, identifier.getObjectId(), format);
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> findDecodeCodec(int dataType, Format format, Class<? extends T> type) {
        DecodeCache cache = this.decodeCodecsCache.get(type);
        if (cache != null) {
            Codec<?> cached = cache.get(dataType, format);
            if (cached != null) {
                return (Codec<T>) cached;
            }
        }

        LOG.trace("[codec-finder dataType={}, format={}, type={}] Decode codec not found in cache", dataType, format, type.getName());
        Codec<T> codec = this.delegate.findDecodeCodec(dataType, format, type);
        if (codec != null) {
            cacheDecode(codec, type, dataType, format);
        }
        return codec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> findEncodeCodec(T value) {
        Class<?> type = value.getClass();
        Codec<?> cached = this.encodeCodecsCache.get(type);
        if (cached != null) {
            return (Codec<T>) cached;
        }

        LOG.trace("[codec-finder type={}] Encode codec not found in cache", type.getName());
        Codec<T> codec = this.delegate.findEncodeCodec(value);
        if (codec != null) {
            this.encodeCodecsCache.putIfAbsent(type, codec);
        }
        return codec;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> findEncodeNullCodec(Class<T> type) {
        Codec<?> cached = this.encodeNullCodecsCache.get(type);
        if (cached != null) {
            return (Codec<T>) cached;
        }

        LOG.trace("[codec-finder type={}] Encode null codec not found in cache", type.getName());
        Codec<T> codec = this.delegate.findEncodeNullCodec(type);
        if (codec != null) {
            this.encodeNullCodecsCache.putIfAbsent(type, codec);
        }
        return codec;
    }

    private void cacheDecode(Codec<?> codec, Class<?> type, int dataType, Format format) {
        // Validate once at population time so that a cache hit can skip canDecode.
        if (!codec.canDecode(dataType, format, type)) {
            return;
        }
        this.decodeCodecsCache.computeIfAbsent(type, t -> new DecodeCache()).putIfAbsent(dataType, format, codec);
    }

    private void cacheEncode(Codec<?> c, Class<?> type) {
        this.encodeCodecsCache.putIfAbsent(type, c);
        if (c.canEncodeNull(type)) {
            this.encodeNullCodecsCache.putIfAbsent(type, c);
        }
    }

    /**
     * Decode codecs for one target type, keyed by {@code (format, dataType)}. Reads are lock-free and do not allocate.
     * Writes copy the map they replace, so readers never observe a map under mutation; they only happen while the cache
     * is being populated.
     */
    static final class DecodeCache {

        private volatile IntObjectMap<Codec<?>> text = new IntObjectHashMap<>();

        private volatile IntObjectMap<Codec<?>> binary = new IntObjectHashMap<>();

        @Nullable
        Codec<?> get(int dataType, Format format) {
            return (format == Format.FORMAT_BINARY ? this.binary : this.text).get(dataType);
        }

        synchronized void putIfAbsent(int dataType, Format format, Codec<?> codec) {
            IntObjectMap<Codec<?>> current = format == Format.FORMAT_BINARY ? this.binary : this.text;
            if (current.containsKey(dataType)) {
                return;
            }
            IntObjectMap<Codec<?>> copy = new IntObjectHashMap<>(current.size() + 1);
            copy.putAll(current);
            copy.put(dataType, codec);
            if (format == Format.FORMAT_BINARY) {
                this.binary = copy;
            } else {
                this.text = copy;
            }
        }

    }

}
