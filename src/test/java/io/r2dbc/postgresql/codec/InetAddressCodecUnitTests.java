/*
 * Copyright 2017-2020 the original author or authors.
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

import io.r2dbc.postgresql.client.EncodedParameter;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static io.r2dbc.postgresql.client.EncodedParameter.NULL_VALUE;
import static io.r2dbc.postgresql.client.ParameterAssert.assertThat;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INET;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.MONEY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.util.ByteBufUtils.encode;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link InetAddressCodec}.
 */
final class InetAddressCodecUnitTests {

    private static final int dataType = INET.getObjectId();

    @Test
    void constructorNoByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InetAddressCodec(null))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void decode() throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("localhost");

        assertThat(new InetAddressCodec(TEST).decode(encode(TEST, inetAddress.getHostAddress()), dataType, FORMAT_TEXT, InetAddress.class))
            .isEqualTo(inetAddress);
    }

    @Test
    void decodeNoByteBuf() {
        assertThat(new InetAddressCodec(TEST).decode(null, dataType, FORMAT_TEXT, InetAddress.class)).isNull();
    }

    @Test
    void doCanDecode() {
        InetAddressCodec codec = new InetAddressCodec(TEST);

        assertThat(codec.doCanDecode(INET, FORMAT_BINARY)).isTrue();
        assertThat(codec.doCanDecode(MONEY, FORMAT_TEXT)).isFalse();
    }

    @Test
    void doCanDecodeNoFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InetAddressCodec(TEST).doCanDecode(VARCHAR, null))
            .withMessage("format must not be null");
    }

    @Test
    void doCanDecodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InetAddressCodec(TEST).doCanDecode(null, FORMAT_TEXT))
            .withMessage("type must not be null");
    }

    @Test
    void doEncode() throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName("localhost");

        assertThat(new InetAddressCodec(TEST).doEncode(inetAddress))
            .hasFormat(FORMAT_TEXT)
            .hasType(INET.getObjectId())
            .hasValue(encode(TEST, inetAddress.getHostAddress()));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new InetAddressCodec(TEST).doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new InetAddressCodec(TEST).encodeNull())
            .isEqualTo(new EncodedParameter(FORMAT_TEXT, INET.getObjectId(), NULL_VALUE));
    }

}
