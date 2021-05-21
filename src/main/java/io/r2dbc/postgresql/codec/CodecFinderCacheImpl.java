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
import io.r2dbc.postgresql.util.Assert;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Cache implementation of the {@link CodecFinder}. This will keep the relevant {@link Codec} for the type, format and database type cached for faster access.
 * In case the {@link Codec} can't be found in the cache, a fallback search using {@link CodecFinderDefaultImpl} will be done.
 */
public class CodecFinderCacheImpl implements CodecFinder {

    private static final Logger LOG = Loggers.getLogger(CodecFinderCacheImpl.class);

    List<Codec<?>> codecs = Collections.emptyList();

    private final Map<Integer, Codec<?>> decodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Integer, Codec<?>> encodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Integer, Codec<?>> encodeNullCodecsCache = new ConcurrentHashMap<>();

    private final CodecFinder fallBackFinder;

    public CodecFinderCacheImpl() {
        this(new CodecFinderDefaultImpl());
    }

    public CodecFinderCacheImpl(CodecFinder fallBackFinder) {
        Assert.requireNonType(fallBackFinder, CodecFinderCacheImpl.class, "fallBackFinder must not be of type CodecFinderCacheImpl");
        this.fallBackFinder = fallBackFinder;
    }

    private static <T> int generateCodecHash(int dataType, Format format, Class<? extends T> type) {
        int hash = (dataType << 5) - dataType;
        hash = (hash << 5) - hash + format.hashCode();
        hash = (hash << 5) - hash + generateCodecHash(type);
        return hash;
    }

    private static <T> int generateCodecHash(Class<? extends T> type) {
        int hash = type.hashCode();
        if (type.getComponentType() != null) {
            hash = (hash << 5) - hash + generateCodecHash(type.getComponentType());
        }
        return hash;
    }

    void invalidateCaches() {
        this.decodeCodecsCache.clear();
        this.encodeCodecsCache.clear();
        this.encodeNullCodecsCache.clear();
        buildCaches();
    }

    void buildCaches() {
        for (Codec<?> c : this.codecs) {
            Optional<Class<?>> arrayClass = Optional.empty();
            if (c instanceof ArrayCodec) {
                ArrayCodec<?> ac = (ArrayCodec<?>) c;
                arrayClass = Optional.of(Array.newInstance(ac.getComponentType(), 0).getClass());
            }
            cacheEncode(c, c.type());
            arrayClass.ifPresent(ac -> cacheEncode(c, ac));
            for (PostgresTypeIdentifier identifier : c.getDataTypes()) {
                for (Format format : c.getFormats()) {
                    cacheDecode(c, c.type(), identifier, format);
                    arrayClass.ifPresent(ac -> cacheDecode(c, ac, identifier, format));
                }
            }
        }
        // Handle decode to Object.class support
        for (PostgresqlObjectId identifier : PostgresqlObjectId.values()) {
            for (Format format : Format.all()) {
                Codec<?> c = fallBackFinder.findDecodeCodec(identifier.getObjectId(), format, Object.class);
                if (c != null) {
                    cacheDecode(c, Object.class, identifier, format);
                }
            }
        }
    }

    private void cacheDecode(Codec<?> c, Class<?> type, PostgresTypeIdentifier identifier, Format format) {
        int decodeHash = generateCodecHash(identifier.getObjectId(), format, type);
        decodeCodecsCache.putIfAbsent(decodeHash, c);
    }

    private void cacheEncode(Codec<?> c, Class<?> type) {
        int encodeHash = generateCodecHash(type);
        encodeCodecsCache.putIfAbsent(encodeHash, c);
        if (c.canEncodeNull(type)) {
            encodeNullCodecsCache.putIfAbsent(encodeHash, c);
        }
    }

    @SuppressWarnings("unchecked")
    synchronized <T> Codec<T> findCodec(int codecHash, Map<Integer, Codec<?>> cache, Supplier<Codec<T>> fallback) {
        return Optional.ofNullable((Codec<T>) cache.get(codecHash)).orElseGet(fallback);
    }

    @Override
    public synchronized void updateCodecs(List<Codec<?>> codecs) {
        this.codecs = codecs;
        fallBackFinder.updateCodecs(codecs);
        invalidateCaches();
    }

    @Override
    public <T> Codec<T> findDecodeCodec(int dataType, Format format, Class<? extends T> type) {
        int hash = generateCodecHash(dataType, format, type);
        return findCodec(hash, decodeCodecsCache, () -> {
            LOG.trace("[codec-finder dataType={}, format={}, type={}] Decode codec not found in cache", dataType, format, type.getName());
            Codec<T> c = fallBackFinder.findDecodeCodec(dataType, format, type);
            if (c != null) {
                decodeCodecsCache.putIfAbsent(hash, c);
            }
            return c;
        });
    }

    @Override
    public <T> Codec<T> findEncodeCodec(T value) {
        int hash = generateCodecHash(value.getClass());
        return findCodec(hash, encodeCodecsCache, () -> {
            LOG.trace("[codec-finder type={}] Encode codec not found in cache", value.getClass().getName());
            Codec<T> c = fallBackFinder.findEncodeCodec(value);
            if (c != null) {
                encodeCodecsCache.putIfAbsent(hash, c);
            }
            return c;
        });
    }

    @Override
    public <T> Codec<T> findEncodeNullCodec(Class<T> type) {
        int hash = generateCodecHash(type);
        return findCodec(hash, encodeNullCodecsCache, () -> {
            LOG.trace("[codec-finder type={}] Encode null codec not found in cache", type.getName());
            Codec<T> c = fallBackFinder.findEncodeNullCodec(type);
            if (c != null) {
                encodeNullCodecsCache.putIfAbsent(hash, c);
            }
            return c;
        });
    }

}
