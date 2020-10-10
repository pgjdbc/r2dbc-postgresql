package io.r2dbc.postgresql.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.util.Assert;
import reactor.util.annotation.Nullable;

import java.util.UUID;
import java.util.function.Supplier;

public class UuidArrayCodec extends AbstractArrayCodec<UUID> {
	UuidArrayCodec(ByteBufAllocator byteBufAllocator) {
		super(byteBufAllocator, UUID.class);
	}

	public Parameter encodeNull() {
		return createNull(PostgresqlObjectId.UUID_ARRAY, Format.FORMAT_TEXT);
	}

	UUID decodeItem(ByteBuf byteBuf) {
		return new UUID(byteBuf.readLong(), byteBuf.readLong());
	}

	UUID decodeItem(String str) {
		return UUID.fromString(str);
	}

	boolean doCanDecode(PostgresqlObjectId type, @Nullable Format format) {
		Assert.requireNonNull(type, "type must not be null");
		return PostgresqlObjectId.UUID_ARRAY==type;
	}

	Parameter encodeArray(Supplier<ByteBuf> encodedSupplier) {
		return create(PostgresqlObjectId.UUID_ARRAY, Format.FORMAT_TEXT, encodedSupplier);
	}

	String encodeItem(UUID value) {
		Assert.requireNonNull(value, "value must not be null");
		return value.toString();
	}
}
