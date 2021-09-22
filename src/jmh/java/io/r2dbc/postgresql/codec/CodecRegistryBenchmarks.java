/*
 * Copyright 2021 the original author or authors.
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
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static io.r2dbc.postgresql.message.Format.FORMAT_BINARY;
import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.FLOAT8_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.INT4_ARRAY;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TIMESTAMP;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;

/**
 * Benchmarks for codec encoding and decoding using cached enabled or disabled registries.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Testable
public class CodecRegistryBenchmarks extends BenchmarkSettings {

    @State(Scope.Benchmark)
    public static class CodecRegistryHolder {

        @Param({"10", "100", "1000"})
        public int iterations;

        final ByteBufAllocator byteBufAllocator = new UnpooledByteBufAllocator(false, true);

        DefaultCodecs cacheEnabledRegistry = new DefaultCodecs(this.byteBufAllocator, false, CachedCodecLookup::new);

        DefaultCodecs cacheDisabledRegistry = new DefaultCodecs(this.byteBufAllocator, false, DefaultCodecLookup::new);

    }

    private void decode(Codecs codecs, int iterations, Blackhole voodoo) {
        for (int i = 0; i < iterations; i++) {
            voodoo.consume(codecs.decode(
                TEST.buffer(4).writeInt(200), INT4.getObjectId(), FORMAT_BINARY, Integer.class));
            voodoo.consume(codecs.decode(
                ByteBufUtils.encode(TEST, "100"), INT2.getObjectId(), FORMAT_TEXT, Short.class));
            voodoo.consume(codecs.decode(
                ByteBufUtils.encode(TEST, "-125.369"), FLOAT8.getObjectId(), FORMAT_TEXT, Double.class));
            voodoo.consume(codecs.decode(
                TEST.buffer(4).writeFloat(-65.369f), FLOAT4.getObjectId(), FORMAT_BINARY, Float.class));
            voodoo.consume(
                codecs.decode(
                    ByteBufUtils.encode(TEST, "test"),
                    VARCHAR.getObjectId(),
                    FORMAT_TEXT,
                    String.class));
            voodoo.consume(
                codecs.decode(
                    ByteBufUtils.encode(TEST, "2018-11-04 15:35:00.847108"),
                    TIMESTAMP.getObjectId(),
                    FORMAT_TEXT,
                    LocalDateTime.class));
            voodoo.consume(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT2_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
            voodoo.consume(codecs.decode(ByteBufUtils.encode(TEST, "{100,200}"), INT4_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
            voodoo.consume(codecs.decode(ByteBufUtils.encode(TEST, "{100.5,200.8}"), FLOAT8_ARRAY.getObjectId(), FORMAT_TEXT, Object.class));
        }
    }

    @Benchmark
    public void decodeWithCacheEnabledRegistry(CodecRegistryHolder holder, Blackhole voodoo) {
        decode(holder.cacheEnabledRegistry, holder.iterations, voodoo);
    }

    @Benchmark
    public void decodeWithCacheDisabledRegistry(CodecRegistryHolder holder, Blackhole voodoo) {
        decode(holder.cacheDisabledRegistry, holder.iterations, voodoo);
    }

    private void encode(Codecs codecs, int iterations, Blackhole voodoo) {
        for (int i = 0; i < iterations; i++) {
            voodoo.consume(codecs.encode((short) 12));
            voodoo.consume(codecs.encode(35698));
            voodoo.consume(codecs.encode(-256.3698));
            voodoo.consume(codecs.encode(85.7458f));
            voodoo.consume(codecs.encode("A text value"));
            voodoo.consume(codecs.encode(LocalDateTime.now()));
            voodoo.consume(codecs.encode(new Long[]{100L, 200L}));
            voodoo.consume(codecs.encode(new Double[]{100.5, 200.25}));
            voodoo.consume(codecs.encodeNull(Integer.class));
            voodoo.consume(codecs.encodeNull(String.class));
            voodoo.consume(codecs.encodeNull(Double[].class));
        }
    }

    @Benchmark
    public void encodeWithCacheEnabledRegistry(CodecRegistryHolder holder, Blackhole voodoo) {
        encode(holder.cacheEnabledRegistry, holder.iterations, voodoo);
    }

    @Benchmark
    public void encodeWithCacheDisabledRegistry(CodecRegistryHolder holder, Blackhole voodoo) {
        encode(holder.cacheDisabledRegistry, holder.iterations, voodoo);
    }

}
