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

/**
 * Codec for a primitive wrapper such as {@link DoubleCodec} for {@link Double}. {@link #getPrimitiveCodec()} is expected to return a {@code double} codec.
 *
 * @since 1.0.6
 */
interface PrimitiveWrapperCodecProvider<T> {

    /**
     * Return the codec for its primitive type.
     *
     * @return the codec for its primitive type
     */
    PrimitiveCodec<T> getPrimitiveCodec();
}
