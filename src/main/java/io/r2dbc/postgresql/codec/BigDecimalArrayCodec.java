/*
 * Copyright 2019-2020 the original author or authors.
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

import java.math.BigDecimal;
import java.util.function.Supplier;

import static io.r2dbc.postgresql.type.PostgresqlObjectId.NUMERIC_ARRAY;

final class BigDecimalArrayCodec extends AbstractArrayCodec<BigDecimal> {

    public BigDecimalArrayCodec(ByteBufAllocator byteBufAllocator) {
        super(byteBufAllocator, BigDecimal.class, NUMERIC_ARRAY);
    }

    @Override
    BigDecimal doDecodeBinary(ByteBuf byteBuffer) {
        return NumericDecodeUtils.decodeBinary(byteBuffer);
    }

    @Override
    BigDecimal doDecodeText(String text) {
        return new BigDecimal(text);
    }

    @Override
    String doEncodeText(BigDecimal value) {
        Assert.requireNonNull(value, "value must not be null");

        return value.toString();
    }

    @Override
    EncodedParameter encodeArray(Supplier<ByteBuf> encodedSupplier) {
        return create(NUMERIC_ARRAY, Format.FORMAT_TEXT, encodedSupplier);
    }

}
