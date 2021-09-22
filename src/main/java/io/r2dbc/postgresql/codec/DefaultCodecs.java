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

package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;

/**
 * The default {@link Codec} implementation.  Delegates to type-specific codec implementations.
 * <p>Codecs can be configured to prefer or avoid attached buffers for certain data types.  Using attached buffers is more memory-efficient as data doesn't need to be copied. In turn, attached
 * buffers require release or consumption to avoid memory leaks.  By default, codecs don't use attached buffers to minimize the risk of memory leaks.</p>
 */
public final class DefaultCodecs implements Codecs, CodecRegistry {

    private final List<Codec<?>> codecs;

    private final CodecLookup codecLookup;

    /**
     * Create a new instance of {@link DefaultCodecs} preferring detached (copied buffers).
     *
     * @param byteBufAllocator the {@link ByteBufAllocator} to use for encoding
     */
    public DefaultCodecs(ByteBufAllocator byteBufAllocator) {
        this(byteBufAllocator, false);
    }

    /**
     * Create a new instance of {@link DefaultCodecs} preferring detached (copied buffers).
     *
     * @param byteBufAllocator      the {@link ByteBufAllocator} to use for encoding
     * @param preferAttachedBuffers whether to prefer attached (pooled) {@link ByteBuf buffers}. Use {@code false} (default) to use detached buffers which minimize the risk of memory leaks.
     */
    public DefaultCodecs(ByteBufAllocator byteBufAllocator, boolean preferAttachedBuffers) {

        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

        this.codecs = getDefaultCodecs(byteBufAllocator, preferAttachedBuffers);
        this.codecLookup = new CachedCodecLookup(new DefaultCodecLookup(this));
    }

    /**
     * Create a new instance of {@link DefaultCodecs}.
     *
     * @param byteBufAllocator      the {@link ByteBufAllocator} to use for encoding
     * @param preferAttachedBuffers whether to prefer attached (pooled) {@link ByteBuf buffers}. Use {@code false} (default) to use detached buffers which minimize the risk of memory leaks.
     * @param codecLookupFunction   provides the {@link CodecLookup} to use for finding relevant codecs
     */
    DefaultCodecs(ByteBufAllocator byteBufAllocator, boolean preferAttachedBuffers, Function<CodecRegistry, CodecLookup> codecLookupFunction) {
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        Assert.requireNonNull(codecLookupFunction, "codecLookupFunction must not be null");

        this.codecLookup = codecLookupFunction.apply(this);
        this.codecs = getDefaultCodecs(byteBufAllocator, preferAttachedBuffers);
        this.codecLookup.afterCodecAdded();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Codec<?>> getDefaultCodecs(ByteBufAllocator byteBufAllocator, boolean preferAttachedBuffers) {

        List<Codec<?>> codecs = new CopyOnWriteArrayList<>(Arrays.asList(

            // Prioritized Codecs
            new StringCodec(byteBufAllocator),
            new InstantCodec(byteBufAllocator),
            new ZonedDateTimeCodec(byteBufAllocator),
            new BinaryByteBufferCodec(byteBufAllocator),
            new BinaryByteArrayCodec(byteBufAllocator),

            new BigDecimalCodec(byteBufAllocator),
            new BigIntegerCodec(byteBufAllocator),
            new BooleanCodec(byteBufAllocator),
            new CharacterCodec(byteBufAllocator),
            new DoubleCodec(byteBufAllocator),
            new FloatCodec(byteBufAllocator),
            new InetAddressCodec(byteBufAllocator),
            new IntegerCodec(byteBufAllocator),
            new IntervalCodec(byteBufAllocator),
            new LocalDateCodec(byteBufAllocator),
            new LocalDateTimeCodec(byteBufAllocator),
            new LocalTimeCodec(byteBufAllocator),
            new LongCodec(byteBufAllocator),
            new OffsetDateTimeCodec(byteBufAllocator),
            new OffsetTimeCodec(byteBufAllocator),
            new ShortCodec(byteBufAllocator),
            new UriCodec(byteBufAllocator),
            new UrlCodec(byteBufAllocator),
            new UuidCodec(byteBufAllocator),
            new ZoneIdCodec(byteBufAllocator),

            // JSON
            new JsonCodec(byteBufAllocator, preferAttachedBuffers),
            new JsonByteArrayCodec(byteBufAllocator),
            new JsonByteBufCodec(byteBufAllocator),
            new JsonByteBufferCodec(byteBufAllocator),
            new JsonInputStreamCodec(byteBufAllocator),
            new JsonStringCodec(byteBufAllocator),

            // Fallback for Object.class
            new ByteCodec(byteBufAllocator),
            new DateCodec(byteBufAllocator),

            new BlobCodec(byteBufAllocator),
            new ClobCodec(byteBufAllocator),
            RefCursorCodec.INSTANCE,
            RefCursorNameCodec.INSTANCE,

            // Array
            new StringArrayCodec(byteBufAllocator),

            // Geometry
            new CircleCodec(byteBufAllocator),
            new PointCodec(byteBufAllocator),
            new BoxCodec(byteBufAllocator),
            new LineCodec(byteBufAllocator),
            new LsegCodec(byteBufAllocator),
            new PathCodec(byteBufAllocator),
            new PolygonCodec(byteBufAllocator)
        ));

        List<Codec<?>> defaultArrayCodecs = new ArrayList<>();

        for (Codec<?> codec : codecs) {

            if (codec instanceof ArrayCodecDelegate<?>) {

                Assert.requireType(codec, AbstractCodec.class, "Codec " + codec + " must be a subclass of AbstractCodec to be registered as generic array codec");
                ArrayCodecDelegate<?> delegate = (ArrayCodecDelegate<?>) codec;
                Class<?> componentType = delegate.type();

                if (codec instanceof BoxCodec) {
                    // BOX[] uses a ';' as a delimiter (i.e. "{(3.7,4.6),(1.9,2.8);(5,7),(1.5,3.3)}")
                    defaultArrayCodecs.add(new ArrayCodec(byteBufAllocator, delegate.getArrayDataType(), delegate, componentType, (byte) ';'));
                } else if (codec instanceof AbstractNumericCodec) {
                    defaultArrayCodecs.add(new ConvertingArrayCodec(byteBufAllocator, delegate, componentType, ConvertingArrayCodec.NUMERIC_ARRAY_TYPES));
                } else if (codec instanceof AbstractTemporalCodec) {
                    defaultArrayCodecs.add(new ConvertingArrayCodec(byteBufAllocator, delegate, componentType, ConvertingArrayCodec.DATE_ARRAY_TYPES));
                } else {
                    defaultArrayCodecs.add(new ArrayCodec(byteBufAllocator, delegate, componentType));
                }
            }
        }

        codecs.addAll(defaultArrayCodecs);

        return codecs;
    }

    @Override
    public void addFirst(Codec<?> codec) {
        Assert.requireNonNull(codec, "codec must not be null");
        this.codecs.add(0, codec);
        this.codecLookup.afterCodecAdded();
    }

    @Override
    public void addLast(Codec<?> codec) {
        Assert.requireNonNull(codec, "codec must not be null");
        this.codecs.add(codec);
        this.codecLookup.afterCodecAdded();
    }

    @Override
    @Nullable
    public <T> T decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends T> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        if (buffer == null) {
            return null;
        }

        Codec<T> codec = this.codecLookup.findDecodeCodec(dataType, format, type);
        if (codec != null) {
            return codec.decode(buffer, dataType, format, type);
        }

        throw new IllegalArgumentException(String.format("Cannot decode value of type %s with OID %d", type.getName(), dataType));
    }

    @Override
    public EncodedParameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        PostgresTypeIdentifier dataType = null;
        Object parameterValue = value;

        if (value instanceof Parameter) {

            Parameter parameter = (Parameter) value;
            parameterValue = parameter.getValue();

            if (parameter.getType() instanceof Type.InferredType && parameterValue == null) {
                return encodeNull(parameter.getType().getJavaType());
            }

            if (parameter.getType() instanceof R2dbcType) {
                dataType = PostgresqlObjectId.valueOf((R2dbcType) parameter.getType());
            }

            if (parameter.getType() instanceof PostgresTypeIdentifier) {
                dataType = (PostgresTypeIdentifier) parameter.getType();
            }
        }

        return encodeParameterValue(value, dataType, parameterValue);
    }

    EncodedParameter encodeParameterValue(Object value, @Nullable PostgresTypeIdentifier dataType, @Nullable Object parameterValue) {
        if (dataType == null) {

            if (parameterValue == null) {
                throw new IllegalArgumentException(String.format("Cannot encode null value %s using type inference", value));
            }

            Codec<?> codec = this.codecLookup.findEncodeCodec(parameterValue);
            if (codec != null) {
                return codec.encode(parameterValue);
            }
        } else {

            if (parameterValue == null) {
                return new EncodedParameter(Format.FORMAT_BINARY, dataType.getObjectId(), NULL_VALUE);
            }

            Codec<?> codec = this.codecLookup.findEncodeCodec(parameterValue);
            if (codec != null) {
                return codec.encode(parameterValue, dataType.getObjectId());
            }
        }

        throw new IllegalArgumentException(String.format("Cannot encode parameter of type %s (%s)", value.getClass().getName(), parameterValue));
    }

    @Override
    public EncodedParameter encodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        Codec<?> codec = this.codecLookup.findEncodeNullCodec(type);
        if (codec != null) {
            return codec.encodeNull();
        }

        throw new IllegalArgumentException(String.format("Cannot encode null parameter of type %s", type.getName()));
    }

    @Override
    public Class<?> preferredType(int dataType, Format format) {
        Assert.requireNonNull(format, "format must not be null");

        Codec<?> codec = this.codecLookup.findDecodeCodec(dataType, format, Object.class);
        if (codec instanceof CodecMetadata) {
            return ((CodecMetadata) codec).type();
        }

        return null;
    }

    @Override
    public Iterator<Codec<?>> iterator() {
        return Collections.unmodifiableList(new ArrayList<>(this.codecs)).iterator();
    }

}
