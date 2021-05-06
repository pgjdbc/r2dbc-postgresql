package io.r2dbc.postgresql.codec;

import org.junit.jupiter.api.Test;

import static io.r2dbc.postgresql.codec.PostgresqlObjectId.INT2_ARRAY;
import static io.r2dbc.postgresql.codec.PostgresqlObjectId.VARCHAR_ARRAY;
import static io.r2dbc.postgresql.util.TestByteBufAllocator.TEST;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class GenericArrayCodecTest {

    @Test
    void constructorWithNullByteBufAllocator() {
        assertThatIllegalArgumentException().isThrownBy(() -> new GenericArrayCodec<>(null, INT2_ARRAY, new ShortCodec(TEST)))
            .withMessage("byteBufAllocator must not be null");
    }

    @Test
    void constructorWithNullOid() {
        assertThatIllegalArgumentException().isThrownBy(() -> new GenericArrayCodec<>(TEST, null, new ShortCodec(TEST)))
            .withMessage("oid must not be null");
    }

    @Test
    void constructorWithNullDelegate() {
        assertThatIllegalArgumentException().isThrownBy(() -> new GenericArrayCodec<>(TEST, INT2_ARRAY, null))
            .withMessage("value must not be null");
    }

    @Test
    void disallowDelegatesWithAbstractArrayCodecType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new GenericArrayCodec<>(TEST, VARCHAR_ARRAY, new StringArrayCodec(TEST)))
            .withMessage("delegate must not be of type GenericArrayCodec");
    }

}