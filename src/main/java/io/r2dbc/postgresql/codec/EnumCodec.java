/*
 * Copyright 2020 the original author or authors.
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
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Codec to map Postgres {@code enumerated} types to Java {@link Enum} values.
 * This codec uses {@link Enum#name()} to map Postgres enum values as these are represented as string values.
 * <p>Note that enum values are case-sensitive.
 *
 * @param <T> enum type
 * @since 0.8.4
 */
public class EnumCodec<T extends Enum<T>> implements Codec<T>, CodecMetadata {

    private static final Logger logger = Loggers.getLogger(EnumCodec.class);

    private final ByteBufAllocator byteBufAllocator;

    private final Class<T> type;

    private final int oid;

    public EnumCodec(ByteBufAllocator byteBufAllocator, Class<T> type, int oid) {
        this.byteBufAllocator = Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.type = Assert.requireNonNull(type, "type must not be null");
        this.oid = oid;
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");
        return type.isAssignableFrom(this.type) && dataType == this.oid;
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");
        return this.type.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");
        return this.type.equals(type);
    }

    @Override
    public T decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends T> type) {
        if (buffer == null) {
            return null;
        }

        return Enum.valueOf(this.type, ByteBufUtils.decode(buffer));
    }

    @Override
    public EncodedParameter encode(Object value) {
        return encode(value, this.oid);
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {

        Assert.requireNonNull(value, "value must not be null");

        return new EncodedParameter(FORMAT_TEXT, dataType, Mono.fromSupplier(() -> ByteBufUtils.encode(this.byteBufAllocator, this.type.cast(value).name())));
    }

    @Override
    public EncodedParameter encodeNull() {
        return encodeNull(this.oid);
    }

    private EncodedParameter encodeNull(int dataType) {
        return new EncodedParameter(Format.FORMAT_BINARY, dataType, NULL_VALUE);
    }

    @Override
    public Class<?> type() {
        return this.type;
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return Collections.singleton(AbstractCodec.getDataType(this.oid));
    }

    /**
     * Create a new {@link Builder} to build a {@link CodecRegistrar} to dynamically register Postgres {@code enum} types to {@link Enum} values.
     *
     * @return a new builder.
     */
    public static EnumCodec.Builder builder() {
        return new Builder();
    }

    static class EnumArrayCodec<T extends Enum<T>> extends EnumCodec<T> implements ArrayCodecDelegate<T> {

        private final PostgresTypeIdentifier arrayType;

        public EnumArrayCodec(ByteBufAllocator byteBufAllocator, Class<T> type, int oid, PostgresTypeIdentifier arrayType) {
            super(byteBufAllocator, type, oid);
            this.arrayType = arrayType;
        }

        @Override
        public String encodeToText(T value) {
            return value.name();
        }

        @Override
        public PostgresTypeIdentifier getArrayDataType() {
            return this.arrayType;
        }

        @Override
        public T decode(ByteBuf buffer, PostgresTypeIdentifier dataType, Format format, Class<? extends T> type) {
            return decode(buffer, dataType.getObjectId(), format, type);
        }

    }

    /**
     * Builder for {@link CodecRegistrar} to register {@link EnumCodec} for one or more enum type mappings.
     */
    public static final class Builder {

        private final Map<String, Class<? extends Enum<?>>> mapping = new LinkedHashMap<>();

        private RegistrationPriority registrationPriority = RegistrationPriority.LAST;

        /**
         * Add a Postgres enum type to {@link Enum} mapping.
         *
         * @param name      name of the Postgres enum type
         * @param enumClass the corresponding Java type
         * @return this {@link Builder}
         */
        public Builder withEnum(String name, Class<? extends Enum<?>> enumClass) {
            Assert.requireNotEmpty(name, "Postgres type name must not be null");
            Assert.requireNonNull(enumClass, "Enum class must not be null");
            Assert.isTrue(enumClass.isEnum(), String.format("Enum class %s must be an enum type", enumClass.getName()));

            if (this.mapping.containsKey(name)) {
                throw new IllegalArgumentException(String.format("Builder contains already a mapping for Postgres type %s", name));
            }

            if (this.mapping.containsValue(enumClass)) {
                throw new IllegalArgumentException(String.format("Builder contains already a mapping for Java type %s", enumClass.getName()));
            }

            this.mapping.put(name, enumClass);
            return this;
        }

        /**
         * Configure the codec registration priority. Default {@link RegistrationPriority#LAST}.
         *
         * @param registrationPriority the registration priority
         * @return this {@link Builder}
         * @throws IllegalArgumentException of {@code registrationPriority} is {@code null}.
         * @since 0.9
         */
        public Builder withRegistrationPriority(RegistrationPriority registrationPriority) {
            this.registrationPriority = Assert.requireNonNull(registrationPriority, "registrationPriority must not be null");
            return this;
        }

        /**
         * Build a {@link CodecRegistrar} to be used with {@code PostgresqlConnectionConfiguration.Builder#codecRegistrar(CodecRegistrar)}.
         * The codec registrar registers the codes to be used as part of the connection setup.
         *
         * @return a new {@link CodecRegistrar}.
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public CodecRegistrar build() {

            Map<String, Class<? extends Enum<?>>> mapping = new LinkedHashMap<>(this.mapping);

            return (connection, allocator, registry) -> {

                List<String> missing = new ArrayList<>(mapping.keySet());
                return PostgresTypes.from(connection).lookupTypes(mapping.keySet())
                    .filter(PostgresTypes.PostgresType::isEnum)
                    .doOnNext(it -> {

                        Class<? extends Enum<?>> enumClass = mapping.get(it.getName());
                        if (enumClass == null) {
                            logger.warn("Cannot find Java type for enum type '{}' with oid {}. Known types are: {}", it.getName(), it.getOid(), mapping);
                            return;
                        }

                        missing.remove(it.getName());
                        logger.debug("Registering codec for type '{}' with oid {} using Java enum type '{}'", it.getName(), it.getOid(), enumClass.getName());

                        if (this.registrationPriority == RegistrationPriority.LAST) {

                            if (it.getArrayObjectId() > 0) {
                                registry.addLast(new ArrayCodec(allocator, new EnumArrayCodec(allocator, enumClass, it.getOid(), it.asArrayType()), enumClass));
                            }

                            registry.addLast(new EnumCodec(allocator, enumClass, it.getOid()));
                        } else {

                            if (it.getArrayObjectId() > 0) {
                                registry.addFirst(new ArrayCodec(allocator, new EnumArrayCodec(allocator, enumClass, it.getOid(), it.asArrayType()), enumClass));
                            }

                            registry.addFirst(new EnumCodec(allocator, enumClass, it.getOid()));
                        }
                    }).doOnComplete(() -> {

                        if (!missing.isEmpty()) {
                            logger.warn("Could not lookup enum types for: {}", missing);
                        }
                    }).then();
            };
        }

        /**
         * An enumeration of codec registration priorities.
         */
        public enum RegistrationPriority {
            FIRST,
            LAST
        }

    }

}
