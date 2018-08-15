/*
 * Copyright 2017-2018 the original author or authors.
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

import io.r2dbc.postgresql.client.Parameter;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.r2dbc.postgresql.message.Format.BINARY;
import static io.r2dbc.postgresql.message.Format.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNSPECIFIED;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

final class InetAddressCodecTest {

    @Test
    void constructorNoByteBufAllocator() {
        assertThatNullPointerException().isThrownBy(() -> new InetAddressCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("localhost");

        assertThat(new InetAddressCodec(TEST).decode(encode(TEST, inetAddress.getHostAddress()), TEXT, InetAddress.class))
            .isEqualTo(inetAddress);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new InetAddressCodec(TEST).decode(null, TEXT, InetAddress.class)).isNull();
    }

    @Test
    void doCanDecode() {
        InetAddressCodec codec = new InetAddressCodec(TEST);

        assertThat(codec.doCanDecode(BINARY, UNSPECIFIED)).isFalse();
        assertThat(codec.doCanDecode(TEXT, MONEY)).isFalse();
        assertThat(codec.doCanDecode(TEXT, UNSPECIFIED)).isTrue();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatNullPointerException().isThrownBy(() -> new InetAddressCodec(TEST).doCanDecode(null, VARCHAR))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatNullPointerException().isThrownBy(() -> new InetAddressCodec(TEST).doCanDecode(TEXT, null))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("localhost");

        assertThat(new InetAddressCodec(TEST).doEncode(inetAddress))
            .isEqualTo(new Parameter(TEXT, UNSPECIFIED.getObjectId(), encode(TEST, inetAddress.getHostAddress())));
    }

    @Test
    void doEncodeNoValue() {
        assertThatNullPointerException().isThrownBy(() -> new InetAddressCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new InetAddressCodec(TEST).encodeNull())
            .isEqualTo(new Parameter(TEXT, UNSPECIFIED.getObjectId(), null));
    }

}
