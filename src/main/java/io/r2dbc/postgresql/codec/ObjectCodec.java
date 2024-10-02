/*
 * Copyright 2024 the original author or authors.
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
import io.r2dbc.postgresql.client.EncodedParameter;
import io.r2dbc.postgresql.message.Format;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.UNSPECIFIED;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

final class ObjectCodec implements Codec<Object> {

    static final ObjectCodec INSTANCE = new ObjectCodec();

    private ObjectCodec() {
    }

    @Override
    public EncodedParameter encodeNull() {
        return AbstractCodec.createNull(FORMAT_TEXT, UNSPECIFIED);
    }

    public EncodedParameter encodeNull(int dataType) {
        return new EncodedParameter(FORMAT_TEXT, dataType, EncodedParameter.NULL_VALUE);
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        return false;
    }

    @Override
    public boolean canEncode(Object value) {
        return false;
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        return Object.class.equals(type);
    }

    @Override
    public Object decode(ByteBuf buffer, int dataType, Format format, Class<?> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EncodedParameter encode(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EncodedParameter encode(Object value, int dataType) {
        throw new UnsupportedOperationException();
    }

}
