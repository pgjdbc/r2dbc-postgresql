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
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Cache implementation of the {@link CodecLookup}. This will keep the relevant {@link Codec} for the type, format and database type cached for faster access.
 * In case the {@link Codec} can't be found in the cache, a fallback search using {@link DefaultCodecLookup} will be done.
 *
 * @since 0.8.9
 */
class CachedCodecLookup implements CodecLookup {

    private static final Logger LOG = Loggers.getLogger(CachedCodecLookup.class);

    private final Map<Integer, Codec<?>> decodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Class<?>, Codec<?>> encodeCodecsCache = new ConcurrentHashMap<>();

    private final Map<Class<?>, Codec<?>> encodeNullCodecsCache = new ConcurrentHashMap<>();

    private final CodecLookup delegate;

    public CachedCodecLookup(Iterable<Codec<?>> codecRegistry) {
        this.delegate = new DefaultCodecLookup(codecRegistry);
    }

    public CachedCodecLookup(CodecLookup delegate) {
        Assert.isTrue(!(delegate instanceof CachedCodecLookup), "delegate must not be of type CachedCodecLookup");
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

            if (c instanceof CodecMetadata) {
                CodecMetadata metadata = (CodecMetadata) c;

                cacheEncode(c, metadata.type());
                for (PostgresqlObjectId identifier : metadata.getDataTypes()) {
                    for (Format format : metadata.getFormats()) {
                        cacheDecode(c, metadata.type(), identifier, format);
                    }
                }
            }
        }
        // Handle decode to Object.class support
        for (PostgresqlObjectId identifier : PostgresqlObjectId.values()) {
            for (Format format : Format.all()) {
                Codec<?> c = this.delegate.findDecodeCodec(identifier.getObjectId(), format, Object.class);
                if (c != null) {
                    cacheDecode(c, Object.class, identifier, format);
                }
            }
        }
    }

    @Override
    public <T> Codec<T> findDecodeCodec(int dataType, Format format, Class<? extends T> type) {
        Integer hash = generateCodecHash(dataType, format, type);
        return findCodec(hash, dataType, format, type, this.decodeCodecsCache, () -> {
            LOG.trace("[codec-finder dataType={}, format={}, type={}] Decode codec not found in cache", dataType, format, type.getName());
            Codec<T> c = this.delegate.findDecodeCodec(dataType, format, type);
            if (c != null) {
                this.decodeCodecsCache.putIfAbsent(hash, c);
            }
            return c;
        });
    }

    @Override
    public <T> Codec<T> findEncodeCodec(T value) {
        return findCodec(value.getClass(), this.encodeCodecsCache, () -> {
            LOG.trace("[codec-finder type={}] Encode codec not found in cache", value.getClass().getName());
            Codec<T> c = this.delegate.findEncodeCodec(value);
            if (c != null) {
                this.encodeCodecsCache.putIfAbsent(value.getClass(), c);
            }
            return c;
        });
    }

    @Override
    public <T> Codec<T> findEncodeNullCodec(Class<T> type) {
        return findCodec(type, this.encodeNullCodecsCache, () -> {
            LOG.trace("[codec-finder type={}] Encode null codec not found in cache", type.getName());
            Codec<T> c = this.delegate.findEncodeNullCodec(type);
            if (c != null) {
                this.encodeNullCodecsCache.putIfAbsent(type, c);
            }
            return c;
        });
    }

    private void cacheDecode(Codec<?> c, Class<?> type, PostgresqlObjectId identifier, Format format) {
        Integer decodeHash = generateCodecHash(identifier.getObjectId(), format, type);
        this.decodeCodecsCache.putIfAbsent(decodeHash, c);
    }

    private void cacheEncode(Codec<?> c, Class<?> type) {
        this.encodeCodecsCache.putIfAbsent(type, c);
        if (c.canEncodeNull(type)) {
            this.encodeNullCodecsCache.putIfAbsent(type, c);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> Codec<T> findCodec(Class<?> cacheKey, Map<Class<?>, Codec<?>> cache, Supplier<Codec<T>> fallback) {
        Codec<T> value = (Codec<T>) cache.get(cacheKey);
        return value != null ? value : fallback.get();
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> Codec<T> findCodec(Integer cacheKey, int dataType, Format format, Class<? extends T> type, Map<Integer, Codec<?>> cache, Supplier<Codec<T>> fallback) {
        Codec<T> value = (Codec<T>) cache.get(cacheKey);
        return (value != null && value.canDecode(dataType, format, type)) ? value : fallback.get();
    }

    private static Integer generateCodecHash(int dataType, Format format, Class<?> type) {
        int result = 1;

        result = 31 * result + dataType;
        result = 31 * result + format.hashCode();
        result = 31 * result + type.hashCode();

        if (type.isArray()) {
            result = 31 * result + type.getComponentType().hashCode();
        }

        return result;
    }

}
