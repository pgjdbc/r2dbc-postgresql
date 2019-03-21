/*
 * Copyright 2017-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 */
public final class DefaultCodecs implements Codecs {

    private final List<Codec<?>> codecs;

    /**
     * Creates a new instance of {@link DefaultCodecs}.
     *
     * @param byteBufAllocator the {@link ByteBufAllocator} to use for encoding
     */
    public DefaultCodecs(ByteBufAllocator byteBufAllocator) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        this.codecs = Arrays.asList(

            // Prioritized Codecs
            new ShortCodec(byteBufAllocator),
            new StringCodec(byteBufAllocator),
            new InstantCodec(byteBufAllocator),
            new ZonedDateTimeCodec(byteBufAllocator),

            new BigDecimalCodec(byteBufAllocator),
            new BooleanCodec(byteBufAllocator),
            new ByteCodec(byteBufAllocator),
            new CharacterCodec(byteBufAllocator),
            new DateCodec(byteBufAllocator),
            new DoubleCodec(byteBufAllocator),
            new EnumCodec(byteBufAllocator),
            new FloatCodec(byteBufAllocator),
            new InetAddressCodec(byteBufAllocator),
            new IntegerCodec(byteBufAllocator),
            new LocalDateCodec(byteBufAllocator),
            new LocalDateTimeCodec(byteBufAllocator),
            new LocalTimeCodec(byteBufAllocator),
            new LongCodec(byteBufAllocator),
            new OffsetDateTimeCodec(byteBufAllocator),
            new UriCodec(byteBufAllocator),
            new UrlCodec(byteBufAllocator),
            new UuidCodec(byteBufAllocator),
            new ZoneIdCodec(byteBufAllocator),

            new ShortArrayCodec(byteBufAllocator),
            new StringArrayCodec(byteBufAllocator),
            new IntegerArrayCodec(byteBufAllocator),
            new LongArrayCodec(byteBufAllocator)
        );
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T decode(@Nullable ByteBuf byteBuf, int dataType, Format format, Class<? extends T> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (byteBuf == null) {
            return null;
        }

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(dataType, format, type)) {
                return ((Codec<T>) codec).decode(byteBuf, format, type);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type %s", type.getName()));
    }

    @Override
    public Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncode(value)) {
                return codec.encode(value);
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode parameter of type %s", value.getClass().getName()));
    }

    @Override
    public Parameter encodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canEncodeNull(type)) {
                return codec.encodeNull();
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode null parameter of type %s", type.getName()));
    }

    @Override
    public Class<?> preferredType(int dataType, Format format) {
        Assert.requireNonNull(format, "format must not be null");

        for (Codec<?> codec : this.codecs) {
            if (codec.canDecode(dataType, format, Object.class)) {
                return codec.type();
            }
        }

        return null;
    }
}
