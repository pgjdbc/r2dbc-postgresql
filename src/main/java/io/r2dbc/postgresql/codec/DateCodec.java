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
import org.jspecify.annotations.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.function.Supplier;

final class DateCodec extends AbstractCodec<Date> implements ArrayCodecDelegate<Date> {

    private final LocalDateTimeCodec delegate;

    private final Supplier<ZoneId> zoneIdSupplier;

    DateCodec(ByteBufAllocator byteBufAllocator, Supplier<ZoneId> zoneIdSupplier) {
        super(Date.class);

        this.delegate = new LocalDateTimeCodec(byteBufAllocator, zoneIdSupplier);
        this.zoneIdSupplier = Assert.requireNonNull(zoneIdSupplier, "zoneIdSupplier must not be null");
    }

    @Override
    public EncodedParameter encodeNull() {
        return this.delegate.encodeNull();
    }

    @Override
    public Iterable<PostgresTypeIdentifier> getDataTypes() {
        return this.delegate.getDataTypes();
    }

    @Override
    boolean doCanDecode(PostgresqlObjectId type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return this.delegate.doCanDecode(type, format);
    }

    @Override
    Date doDecode(ByteBuf buffer, PostgresTypeIdentifier dataType, @Nullable Format format, @Nullable Class<? extends Date> type) {
        Assert.requireNonNull(buffer, "byteBuf must not be null");

        LocalDateTime intermediary = this.delegate.doDecode(buffer, dataType, format, LocalDateTime.class);
        return Date.from(intermediary.atZone(this.zoneIdSupplier.get()).toInstant());
    }

    @Override
    EncodedParameter doEncode(Date value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(normalize(value));
    }

    @Override
    EncodedParameter doEncode(Date value, PostgresTypeIdentifier dataType) {
        Assert.requireNonNull(value, "value must not be null");

        return this.delegate.doEncode(normalize(value), dataType);
    }

    @Override
    public String encodeToText(Date value) {
        Assert.requireNonNull(value, "value must not be null");

        return normalize(value).toString();
    }

    @Override
    public PostgresTypeIdentifier getArrayDataType() {
        return this.delegate.getArrayDataType();
    }

    private LocalDateTime normalize(Date value) {
        return value.toInstant().atZone(this.zoneIdSupplier.get()).toLocalDateTime();
    }

}
