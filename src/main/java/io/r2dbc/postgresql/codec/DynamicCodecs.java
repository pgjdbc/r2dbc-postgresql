package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

public class DynamicCodecs implements CodecRegistrar {

    private enum AvailableCodecs {
        HSTORE("hstore");
        private final String name;

        AvailableCodecs(String name) {
            this.name = name;
        }

        public Codec<?> createCodec(ByteBufAllocator byteBufAllocator, int oid) {
            switch (this) {
                case HSTORE:
                    return new HStoreCodec(byteBufAllocator, oid);
                default:
                    return null;
            }
        }
    }

    @Override
    public Publisher<Void> register(PostgresqlConnection connection, ByteBufAllocator byteBufAllocator, CodecRegistry registry) {
        List<Publisher<Void>> publishers = new ArrayList<>();
        for (AvailableCodecs availableCodecs : AvailableCodecs.values()) {
            publishers.add(
                connection.createStatement("SELECT oid FROM pg_catalog.pg_type WHERE typname = $1")
                    .bind("$1", availableCodecs.name)
                    .execute()
                    .flatMap(it -> it.map((row, rowMetadata) -> {
                            Integer oid = row.get("oid", Integer.class);
                            if (oid != null) {
                                registry.addLast(availableCodecs.createCodec(byteBufAllocator, oid));
                            }
                            return Mono.empty();
                        })
                    ).then()
            );
        }
        return Flux.concat(publishers);
    }
}
