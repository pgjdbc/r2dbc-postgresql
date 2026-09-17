/*
 * Copyright 2026 the original author or authors.
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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.r2dbc.postgresql.BenchmarkSettings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

/**
 * Benchmarks for {@link CachedCodecLookup} cache hits in isolation (no decoding, no buffer allocation).
 * Every {@code row.get(...)} and every bound parameter goes through one of these lookups.
 */
public class CodecLookupBenchmarks extends BenchmarkSettings {

    @State(Scope.Benchmark)
    public static class LookupHolder {

        final ByteBufAllocator byteBufAllocator = new UnpooledByteBufAllocator(false, true);

        final ZoneId zoneId = ZoneId.systemDefault();

        final DefaultCodecs codecs = new DefaultCodecs(this.byteBufAllocator, false, () -> this.zoneId, DefaultCodecLookup::new);

        final CachedCodecLookup lookup;

        final Integer intValue = 35698;

        final String stringValue = "A text value";

        final LocalDateTime timestampValue = LocalDateTime.now();

        final Long[] longArrayValue = {100L, 200L};

        public LookupHolder() {
            this.lookup = new CachedCodecLookup(this.codecs);
            this.lookup.afterCodecAdded();
        }

    }

    @Benchmark
    public Object findDecodeCodecInt4Binary(LookupHolder holder) {
        return holder.lookup.findDecodeCodec(INT4.getObjectId(), FORMAT_BINARY, Integer.class);
    }

    @Benchmark
    @OperationsPerInvocation(9)
    public void findDecodeCodecMixed(LookupHolder holder, Blackhole voodoo) {
        CachedCodecLookup lookup = holder.lookup;
        voodoo.consume(lookup.findDecodeCodec(INT4.getObjectId(), FORMAT_BINARY, Integer.class));
        voodoo.consume(lookup.findDecodeCodec(INT2.getObjectId(), FORMAT_TEXT, Short.class));
        voodoo.consume(lookup.findDecodeCodec(FLOAT8.getObjectId(), FORMAT_TEXT, Double.class));
        voodoo.consume(lookup.findDecodeCodec(FLOAT4.getObjectId(), FORMAT_BINARY, Float.class));
        voodoo.consume(lookup.findDecodeCodec(VARCHAR.getObjectId(), FORMAT_TEXT, String.class));
        voodoo.consume(lookup.findDecodeCodec(TIMESTAMP.getObjectId(), FORMAT_TEXT, LocalDateTime.class));
        voodoo.consume(lookup.findDecodeCodec(INT2_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
        voodoo.consume(lookup.findDecodeCodec(INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
        voodoo.consume(lookup.findDecodeCodec(FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
    }

    @Benchmark
    @OperationsPerInvocation(6)
    public void findEncodeCodecMixed(LookupHolder holder, Blackhole voodoo) {
        CachedCodecLookup lookup = holder.lookup;
        voodoo.consume(lookup.findEncodeCodec(holder.intValue));
        voodoo.consume(lookup.findEncodeCodec(holder.stringValue));
        voodoo.consume(lookup.findEncodeCodec(holder.timestampValue));
        voodoo.consume(lookup.findEncodeCodec(holder.longArrayValue));
        voodoo.consume(lookup.findEncodeNullCodec(Integer.class));
        voodoo.consume(lookup.findEncodeNullCodec(String.class));
    }

}
